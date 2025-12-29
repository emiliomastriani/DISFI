import time
import random
import math
from datetime import datetime, timedelta
from cassandra.cluster import Cluster
from cassandra.policies import DCAwareRoundRobinPolicy
import threading
import argparse
import logging
import errno  # AGGIUNTO per gestire errori di sistema

# =============== CONFIGURAZIONE ARGOMENTI =================
parser = argparse.ArgumentParser()
parser.add_argument('--hosts', default=['storage'], nargs='+', help="Cassandra nodes")
parser.add_argument('--keyspace', default='sensor_traffic', help="Keyspace name")
parser.add_argument('--table', default='telemetry_data', help="Table name")
parser.add_argument('--threads', type=int, default=20, help="Base concurrent clients")
parser.add_argument('--base_ops', type=int, default=50, help="Base operations/sec")
parser.add_argument('--fault_probability', type=float, default=0.01, help="Probability of fault")
parser.add_argument('--num_sensors', type=int, default=1000, help="Number of sensors to simulate")
parser.add_argument('--jitter', type=float, default=0.3, help="Randomness")
parser.add_argument('--enable_read_storm', action='store_true', help="Enable read traffic generator")
parser.add_argument('--hotspot', action='store_true', help="Only write on hot_sensor")
parser.add_argument('--connection_storm', action='store_true', help="Reopen connections every cycle")
parser.add_argument('--cpu_spike', action='store_true', help="Inject artificial CPU load")
parser.add_argument('--duration', type=int, help="Duration of the test in minutes (optional)")
parser.add_argument('--leak_mode', action='store_true', help="LEAK MODE: Never close connections")
args = parser.parse_args()

# =============== LOGGING =====================

logger = logging.getLogger("TelemetrySim")
logger.setLevel(logging.INFO)

# ---- Fault logger ----
fault_logger = logging.getLogger("FaultLogger")
fault_handler = logging.FileHandler("faults.log")
fault_handler.setLevel(logging.WARNING)
fault_handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
fault_logger.addHandler(fault_handler)
fault_logger.propagate = False

# ---- Log su file ----
file_handler = logging.FileHandler("telemetry.log")
file_handler.setLevel(logging.INFO)
formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)

# ---- Log su console ----
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)


# =============== METRICHE =======================
METRICS = {
    'ws_cbattvolt_2': {'unit': 'V', 'simulate': lambda t: 12.0 + random.gauss(0, 0.1)},
    'ws_temp': {'unit': 'C', 'simulate': lambda t: 20 + 5 * math.sin(t / 60.0) + random.gauss(0, 0.2)},
    'ws_wind_speed': {'unit': 'm/s', 'simulate': lambda t: max(0, random.gauss(5, 1))},
    'ws_humidity': {'unit': '%', 'simulate': lambda t: 50 + 10 * math.sin(t / 120.0) + random.gauss(0, 1)}
}

# =============== SCHEMA CASSANDRA ========================
schema = f"""
CREATE KEYSPACE IF NOT EXISTS {args.keyspace}
WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': 1}};

CREATE TABLE IF NOT EXISTS {args.keyspace}.{args.table} (
    sensor_id text,
    event_time timestamp,
    metric_name text,
    value double,
    unit text,
    PRIMARY KEY ((sensor_id), event_time, metric_name)
) WITH CLUSTERING ORDER BY (event_time DESC);
"""

# =============== CPU SPIKE =======================
def cpu_spike_func():
    for _ in range(5_000_000):
        math.sqrt(random.random())

# =============== READ TRAFFIC =======================
def simulate_read(session):
    sensor = random.randint(0, args.num_sensors-1)
    sid = f"sensor_{sensor:03}"
    try:
        session.execute(
            f"SELECT * FROM {args.keyspace}.{args.table} WHERE sensor_id=%s LIMIT 10",
            (sid,)
        )
    except Exception as e:
        logger.error(f"Read failed: {e}")

# =============== WRITE TRAFFIC ======================
def simulate_sensor(sensor_id):
    # Se non siamo in connection_storm, creiamo UNA connessione
    if not args.connection_storm:
        cluster = Cluster(
            contact_points=args.hosts,
            protocol_version=4,
            load_balancing_policy=DCAwareRoundRobinPolicy(local_dc='datacenter1')
        )
        session = cluster.connect()
    else:
        # In connection_storm mode, inizializziamo a None
        cluster = None
        session = None

    start_time = datetime.utcnow()
    
    # Lista per accumulare connessioni in leak mode
    leaked_clusters = []
    leaked_sessions = []

    while True:
        if args.duration:
            elapsed = datetime.utcnow() - start_time
            if elapsed.total_seconds() >= args.duration * 60:
                logger.info(f"Duration {args.duration} minutes reached. Thread {sensor_id} exiting...")
                break

        # Se siamo in connection_storm, creiamo una NUOVA connessione ogni ciclo
        if args.connection_storm:
            try:
                cluster = Cluster(
                    contact_points=args.hosts,
                    protocol_version=4,
                    load_balancing_policy=DCAwareRoundRobinPolicy(local_dc='datacenter1')
                )
                # MODIFICA: Aggiunto try-except specifico per cluster.connect()
                session = cluster.connect()
            except Exception as e:
                # Cattura errori specifici del sistema
                error_str = str(e)
                if hasattr(e, 'errno') and e.errno == errno.EMFILE:
                    logger.error(f"TOO MANY OPEN FILES (EMFILE) nel thread {sensor_id}")
                elif "too many open files" in error_str.lower():
                    logger.error(f"TOO MANY OPEN FILES nel thread {sensor_id}")
                elif "OperationTimedOut" in error_str:
                    logger.error(f"Timeout nel thread {sensor_id} - Cassandra saturata")
                elif "Unable to connect" in error_str:
                    logger.error(f"Unable to connect nel thread {sensor_id} - Cassandra non raggiungibile")
                else:
                    logger.error(f"Thread {sensor_id} connection error: {e}")
                # Aspetta un po' e riprova
                time.sleep(1)
                continue
            
            # In leak mode, teniamo traccia delle connessioni senza chiuderle
            if args.leak_mode:
                leaked_clusters.append(cluster)
                leaked_sessions.append(session)
                if len(leaked_clusters) % 100 == 0:
                    logger.warning(f"LEAK MODE: Thread {sensor_id} has accumulated {len(leaked_clusters)} connections!")

        try:
            timestamp = datetime.utcnow()

            for metric, props in METRICS.items():
                value = props['simulate'](time.time())
                fault_type = None

                if random.random() < args.fault_probability:
                    fault_type = random.choice(["zero", "spike", "nan"])
                    if fault_type == "zero":
                        value = 0.0
                    elif fault_type == "spike":
                        value *= 10
                    elif fault_type == "nan":
                        value = float('nan')
                    fault_logger.warning(
                        f"{datetime.utcnow()} | Sensor={sensor_id} | Metric={metric} | Fault={fault_type} | Value={value}"
                    )

                try:
                    session.execute(
                        f"INSERT INTO {args.keyspace}.{args.table} "
                        "(sensor_id, event_time, metric_name, value, unit) "
                        "VALUES (%s, %s, %s, %s, %s)",
                        (sensor_id, timestamp, metric, value, props['unit'])
                    )
                except Exception as e:
                    # MODIFICA: Anche qui controllo errori specifici
                    error_str = str(e)
                    if "too many open files" in error_str.lower():
                        logger.error(f"INSERT ERROR - TOO MANY OPEN FILES nel thread {sensor_id}")
                    else:
                        logger.error(f"Insertion failed: {e}")

        except Exception as e:
            # MODIFICA: Gestione errori migliorata
            error_str = str(e)
            if "too many open files" in error_str.lower():
                logger.error(f"GENERAL ERROR - TOO MANY OPEN FILES nel thread {sensor_id}")
            else:
                logger.error(f"Thread {sensor_id} error: {e}")

        finally:
            # Chiudiamo la connessione SOLO se:
            # 1. Siamo in connection_storm mode
            # 2. NON siamo in leak_mode
            if args.connection_storm and not args.leak_mode and cluster:
                try:
                    cluster.shutdown()
                except:
                    pass

        if args.enable_read_storm:
            simulate_read(session)

        if args.cpu_spike:
            cpu_spike_func()

        base = 1.0 / args.base_ops
        #sleep = base * (1 + random.uniform(-args.jitter, args.jitter))
        # Sostituzione:
        jitter_factor = random.uniform(max(-0.99, -args.jitter), args.jitter)  # Impedisci -1.0
        sleep = base * (1 + jitter_factor)
        sleep = max(0.001, sleep)  # Assicura minimo 1ms
        time.sleep(sleep)
    
    # Cleanup: chiudiamo tutto alla fine (tranne in leak mode)
    if not args.leak_mode:
        if not args.connection_storm and cluster:
            try:
                cluster.shutdown()
            except:
                pass
        # Chiudiamo anche le connessioni accumulate in leak mode (se usciti prima)
        for cl in leaked_clusters:
            try:
                cl.shutdown()
            except:
                pass

# =============== MAIN ========================
if __name__ == "__main__":
    start_time = datetime.utcnow()
    logger.info(f"=== SCRIPT START ===")
    logger.info(f"Start time: {start_time}")
    logger.info(f"Parameters: {vars(args)}")

    # Connessione iniziale per creare schema
    try:
        cluster = Cluster(
            contact_points=args.hosts,
            protocol_version=4,
            load_balancing_policy=DCAwareRoundRobinPolicy(local_dc='datacenter1')
        )
        session = cluster.connect()
        
        for stmt in schema.split(';'):
            if stmt.strip():
                try:
                    session.execute(stmt)
                except Exception as e:
                    logger.warning(f"Schema statement may have failed: {e}")
    except Exception as e:
        logger.error(f"Initial connection failed: {e}")
        # Esci se non riesci a connetterti all'inizio
        exit(1)

    threads = []

    if args.hotspot:
        logger.info("HOTSPOT ENABLED on `hot_sensor`")
        t = threading.Thread(target=simulate_sensor, args=('hot_sensor',), daemon=True)
        t.start()
        threads.append(t)
    else:
        # USA IL PARAMETRO --threads invece di --num_sensors per il numero di thread!
        num_threads_to_create = args.threads  # Usiamo threads invece di num_sensors
        
        for i in range(num_threads_to_create):
            sensor_id = f"sensor_{i:06}"  # 6 cifre per supportare molti thread
            t = threading.Thread(target=simulate_sensor, args=(sensor_id,), daemon=True)
            t.start()
            threads.append(t)
            if i % 100 == 0:
                logger.info(f"Started {i} threads...")
                time.sleep(0.1)  # Piccola pausa per non sovraccaricare subito

    logger.info(f"Started {len(threads)} threads total")

    try:
        while True:
            if args.duration:
                elapsed = (datetime.utcnow() - start_time).total_seconds()
                if elapsed >= args.duration * 60:
                    logger.info("Duration reached, shutting down main thread.")
                    break
            time.sleep(10)
            logger.info(f"Still running... {len(threads)} threads active")
    except KeyboardInterrupt:
        logger.info("Shutting down...")

    stop_time = datetime.utcnow()
    duration_actual = stop_time - start_time
    logger.info(f"Stop time: {stop_time}")
    logger.info(f"Duration actual: {duration_actual}")
    logger.info(f"=== SCRIPT END ===")

    for t in threads:
        t.join(timeout=5)

    try:
        cluster.shutdown()
    except:
        pass
