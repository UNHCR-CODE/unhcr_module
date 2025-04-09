"""
    # 1. Install Alembic:
# pip install alembic

# 2. Initialize Alembic:
# alembic init migrations

# 3. Configure alembic.ini (migrations/alembic.ini):
# sqlalchemy.url = postgresql://user:password@host/database
# script_location = migrations

# 4. Modify migrations/env.py:
# from logging.config import fileConfig
# from sqlalchemy import engine_from_config
# from sqlalchemy import pool
# from alembic import context
# import models  # Import your SQLAlchemy models
# from models import Base # Import your Base

# config = context.config
# fileConfig(config.config_file_name)
# target_metadata = Base.metadata # Add this line

# def run_migrations_online():
#     connectable = engine_from_config(
#         config.get_section(config.config_ini_section),
#         prefix="sqlalchemy.",
#         poolclass=pool.NullPool,
#     )
#     with connectable.connect() as connection:
#         context.configure(
#             connection=connection, target_metadata=target_metadata
#         )
#         with context.begin_transaction():
#             context.run_migrations()

# run_migrations_online()

# 5. Generate a migration script:
# alembic revision --autogenerate -m "Add created and updated columns to devices table"

# 6. Modify the generated migration script (migrations/versions/*.py):
# from alembic import op
# import sqlalchemy as sa

# def upgrade():
#     op.add_column('devices', sa.Column('created', sa.DateTime(), nullable=True))
#     op.add_column('devices', sa.Column('updated', sa.DateTime(), nullable=True))
#     op.execute("UPDATE solarman.devices SET created = NOW(), updated = NOW()") # populate existing rows.
#     op.alter_column('devices', 'created', nullable=False)
#     op.alter_column('devices', 'updated', nullable=False)
#     op.alter_column('devices', 'created', server_default=sa.func.now())
#     op.alter_column('devices', 'updated', server_default=sa.func.now(), onupdate=sa.func.now())

# def downgrade():
#     op.drop_column('devices', 'updated')
#     op.drop_column('devices', 'created')

# 7. Run the migration:
# alembic upgrade head

# 8. (Optional) Downgrade the migration:
# alembic downgrade -1

For Production:
    alembic -c alembic_azure.ini upgrade head
"""


from datetime import datetime, timezone
import sys
from sqlalchemy import TIMESTAMP, BigInteger, Column, ForeignKey, Index, Integer, Float, String, DateTime, JSON, Numeric, UniqueConstraint, create_engine, func, inspect, select, text
from sqlalchemy.orm import declarative_base, Session

from unhcr import db

from sqlalchemy import MetaData
from sqlalchemy.schema import CreateSchema

# Define naming convention for constraints
convention = {
    "ix": "idx_%(table_name)s_%(column_0_name)s",
    "uq": "unique_%(table_name)s_%(column_0_name)s",
    "ck": "check_%(table_name)s_%(constraint_name)s",
    "fk": "fk_%(table_name)s_%(column_0_name)s",
    "pk": "pkey_%(table_name)s"
}

# Create metadata with naming convention
metadata = MetaData(naming_convention=convention, schema="solarman")

Base = declarative_base(metadata=metadata)

class AlembicVersion(Base):
    __tablename__ = 'alembic_version'
    #__table_args__ = {'schema': 'solarman'}  # Specify the schema

    version_num = Column(String(32), primary_key=True, nullable=False)

class Station(Base):
    __tablename__ = 'stations'
    __table_args__ = (
        Index(None, 'name'),
        # No explicit name - will use convention
        UniqueConstraint('name'),
    )
    # (
    #     Index('idx_stations_name', 'name'),
    #     UniqueConstraint('name', name='uq_station_name'),
    #     {'schema': 'solarman'}
    # )
    id = Column(BigInteger, primary_key=True, autoincrement=False)
    name = Column(String(255), nullable=False)
    location_lat = Column(Float, nullable=True)
    location_lng = Column(Float, nullable=True)
    location_address = Column(String(255), nullable=True)
    region_nation_id = Column(Integer, nullable=True)
    region_level1 = Column(Integer, nullable=True)
    region_level2 = Column(Integer, nullable=True)
    region_level3 = Column(Integer, nullable=True)
    region_level4 = Column(Integer, nullable=True)
    region_level5 = Column(Integer, nullable=True)
    region_timezone = Column(String(50), nullable=True)
    type = Column(String(50), nullable=True)
    grid_interconnection_type = Column(String(50), nullable=True)
    installed_capacity = Column(Float, nullable=True)
    start_operating_time = Column(TIMESTAMP, nullable=True)
    station_image = Column(String(255), nullable=True)
    created_date = Column(TIMESTAMP, nullable=True)
    battery_soc = Column(Float, nullable=True)
    network_status = Column(String(50), nullable=True)
    generation_power = Column(Float, nullable=True)
    last_update_time = Column(TIMESTAMP, nullable=True)
    contact_phone = Column(String(50), nullable=True)
    owner_name = Column(String(255), nullable=True)
    created = Column(DateTime, nullable=False, server_default=func.now())
    updated = Column(DateTime, nullable=False, server_default=func.now(), onupdate=func.now())

class StationData(Base):
    __tablename__ = "station_data_daily"
    __table_args__ = (
        Index(None, 'station_id'),  # Index on 'sn' column
        Index(None, 'ts'),  # Index on 'collect_time' column

        # Index('idx_station_data_id', 'station_id'),  # Index on 'sn' column
        # Index('idx_station_data_ts', 'ts'),  # Index on 'collect_time' column
        # {"schema": "solarman"}  # Schema argument should be a dictionary, placed last
    )
    station_id = Column(BigInteger,  primary_key=True, autoincrement=False, nullable=False)
    ts = Column(DateTime, primary_key=True)

    year = Column(Integer, nullable=False)
    month = Column(Integer, nullable=False)
    day = Column(Integer, nullable=False)

    generation_power = Column(Float)
    use_power = Column(Float)
    grid_power = Column(Float)
    purchase_power = Column(Float)
    wire_power = Column(Float)
    charge_power = Column(Float)
    discharge_power = Column(Float)
    battery_power = Column(Float)
    battery_soc = Column(Float)
    irradiate_intensity = Column(Float)
    generation_value = Column(Float)
    generation_ratio = Column(Float)
    grid_ratio = Column(Float)
    charge_ratio = Column(Float)
    use_value = Column(Float)
    use_ratio = Column(Float)
    buy_ratio = Column(Float)
    use_discharge_ratio = Column(Float)
    grid_value = Column(Float)
    buy_value = Column(Float)
    charge_value = Column(Float)
    discharge_value = Column(Float)
    full_power_hours = Column(Float)
    irradiate = Column(Float)
    theoretical_generation = Column(Float)
    pr = Column(Float)
    cpr = Column(Float)
    created = Column(DateTime, nullable=False, server_default=func.now())
    updated = Column(DateTime, nullable=False, server_default=func.now(), onupdate=func.now())

class Device(Base):
    __tablename__ = "devices"
    __table_args__ = (
        Index(None, 'device_sn'),
        Index(None, 'device_id'),

        # Index('idx_devices_device_sn', 'device_sn'),
        # Index('idx_devices_device_id', 'device_id'),
        # {"schema": "solarman"}
    )

    device_sn = Column(String(25), nullable=False, primary_key=True) 
    device_id = Column(BigInteger, nullable=False)
    device_type = Column(String(50), nullable=False)
    connect_status = Column(Integer, nullable=False)
    collection_time = Column(BigInteger, nullable=False)  # Epoch time stored as BigInteger
    created = Column(DateTime, nullable=False, server_default=func.now())
    updated = Column(DateTime, nullable=False, server_default=func.now(), onupdate=func.now())

class DeviceSiteHistory(Base):
    __tablename__ = "device_site_history"
    __table_args__ = (
        Index(None, 'device_sn'),
        # Index('idx_device_site_history_device_sn', 'device_sn'),
        # {"schema": "solarman"}
    )
    station_id = Column(BigInteger, ForeignKey("solarman.stations.id"), primary_key=True, nullable=False)
    device_sn = Column(String(25), ForeignKey("solarman.devices.device_sn"), primary_key=True, nullable=False)
    device_id = Column(BigInteger, nullable=False)
    comment = Column(String(255), nullable=True)
    start_time = Column(DateTime, primary_key=True, nullable=False)
    end_time = Column(DateTime) #null for current site
    created = Column(DateTime, nullable=False, server_default=func.now())
    updated = Column(DateTime, nullable=False, server_default=func.now(), onupdate=func.now())

class InverterData(Base):
    __tablename__ = 'inverter_data'
    __table_args__ = (
        Index(None, 'device_sn'),
        Index(None, 'ts'),
        # Index('idx_inverter_data_device_sn', 'device_sn'),
        # Index('idx_inverter_data_ts', 'ts'),
        # {"schema": "solarman"}  # Schema argument should be a dictionary, placed last
    )

    ts = Column(DateTime, primary_key=True)
    device_sn = Column(String(25), ForeignKey("solarman.devices.device_sn"), primary_key=True, nullable=False)
    device_id = Column(BigInteger)
    inverter_type = Column(String(255))
    output_power_level = Column(String(255))
    rated_power = Column(Numeric)
    parallel_information = Column(String(255))
    device_type = Column(JSON)
    system_time = Column(DateTime)
    protocol_version = Column(String(255))
    main_data = Column(String(255))
    hmi = Column(String(255))
    lithium_battery_version_number = Column(String(255))
    control_board_activator_version_number = Column(String(255))
    control_board_assisted_microcontroller_version_number = Column(String(255))
    arc_board_firmware_version = Column(String(255))
    dc_voltage_pv1 = Column(Float)
    dc_voltage_pv2 = Column(Float)
    dc_voltage_pv3 = Column(Float)
    dc_voltage_pv4 = Column(Float)
    dc_current_pv1 = Column(Float)
    dc_current_pv2 = Column(Float)
    dc_current_pv3 = Column(Float)
    dc_current_pv4 = Column(Float)
    dc_power_pv1 = Column(Float)
    dc_power_pv2 = Column(Float)
    dc_power_pv3 = Column(Float)
    dc_power_pv4 = Column(Float)
    total_production_active = Column(Float)
    ac_voltage_r_u_a = Column(Float)
    ac_voltage_s_v_b = Column(Float)
    ac_voltage_t_w_c = Column(Float)
    ac_current_r_u_a = Column(Float)
    ac_current_s_v_b = Column(Float)
    ac_current_t_w_c = Column(Float)
    ac_output_frequency_r = Column(Float)
    cumulative_production_active = Column(Float)
    daily_production_active = Column(Float)
    inverter_output_power_l1 = Column(Float)
    inverter_output_power_l2 = Column(Float)
    inverter_output_power_l3 = Column(Float)
    total_inverter_output_power = Column(Float)
    total_solar_power = Column(Float)
    grid_voltage_l1 = Column(Float)
    grid_current_l1 = Column(Float)
    grid_power_l1 = Column(Float)
    grid_voltage_l2 = Column(Float)
    grid_current_l2 = Column(Float)
    grid_power_l2 = Column(Float)
    grid_voltage_l3 = Column(Float)
    grid_current_l3 = Column(Float)
    grid_power_l3 = Column(Float)
    grid_status = Column(String(255))
    external_ct1_power = Column(Float)
    external_ct2_power = Column(Float)
    external_ct3_power = Column(Float)
    total_external_ct_power = Column(Float)
    grid_frequency = Column(Float)
    total_grid_power = Column(Float)
    total_grid_reactive_power = Column(Float)
    a_phase_reactive_power_of_power_grid = Column(Float)
    b_phase_reactive_power_of_power_grid = Column(Float)
    c_phase_reactive_power_of_power_grid = Column(Float)
    daily_energy_buy = Column(Float)
    daily_energy_sell = Column(Float)
    total_energy_buy = Column(Float)
    total_energy_sell = Column(Float)
    internal_l1_power = Column(Float)
    internal_l2_power = Column(Float)
    internal_l3_power = Column(Float)
    internal_power = Column(Float)
    inverter_a_phase_reactive_power = Column(Float)
    inverted_b_phase_reactive_power = Column(Float)
    inverted_c_phase_reactive_power = Column(Float)
    mppt_number_of_routes_and_phases = Column(String(255))
    load_voltage_l1 = Column(Float)
    load_voltage_l2 = Column(Float)
    load_voltage_l3 = Column(Float)
    load_power_l1 = Column(Float)
    load_power_l2 = Column(Float)
    load_power_l3 = Column(Float)
    total_consumption_power = Column(Float)
    total_consumption_apparent_power = Column(Float)
    daily_consumption = Column(Float)
    total_consumption = Column(Float)
    load_frequency = Column(Float)
    load_phase_power_a = Column(Float)
    load_phase_power_b = Column(Float)
    load_phase_power_c = Column(Float)
    battery_status = Column(String(255))
    battery_voltage = Column(Float)
    battery_power1 = Column(Float)
    battery_current1 = Column(Float)
    battery_current2 = Column(Float)
    battery_power = Column(Float)
    soc = Column(Float)
    total_charging_energy = Column(Float)
    total_discharging_energy = Column(Float)
    daily_charging_energy = Column(Float)
    daily_discharging_energy = Column(Float)
    battery_rated_capacity = Column(Float)
    battery_type = Column(String(255))
    battery_mode = Column(JSON)
    battery_factory = Column(String(255))
    battery_1_status = Column(String(255))
    battery_total_current = Column(Float)
    battery_2_status = Column(String(255))
    bms_voltage = Column(Float)
    bms_current = Column(Float)
    bms_temperature = Column(Float)
    bms_charge_voltage = Column(Float)
    bms_discharge_voltage = Column(Float)
    charge_current_limit = Column(Float)
    discharge_current_limit = Column(Float)
    bms_soc = Column(Float)
    bms_charging_max_current = Column(Float)
    bms_discharging_max_current = Column(Float)
    li_bat_flag = Column(String(255))
    temperature_battery = Column(Float)
    ac_temperature = Column(Float)
    year = Column(Integer)
    month = Column(Integer)
    day = Column(Integer)
    hour = Column(Integer)
    minute = Column(Integer)
    second = Column(Integer)
    inverter_algebra = Column(String(255))
    inverter_series_distinction = Column(String(255))
    gs_a1 = Column(Float)
    gs_b1 = Column(Float)
    gs_c1 = Column(Float)
    gs_t1 = Column(Float)
    grid_relay_status = Column(String(255))
    inverter_power_generation_status = Column(String(255))
    gen_power_l1 = Column(Float)
    gen_power_l2 = Column(Float)
    gen_power_l3 = Column(Float)
    gen_voltage_l1 = Column(Float)
    gen_voltage_l2 = Column(Float)
    gen_voltage_l3 = Column(Float)
    gen_daily_run_time = Column(Float)
    generator_active_power = Column(Float)
    total_gen_power = Column(Float)
    daily_production_generator = Column(Float)
    total_production_generator = Column(Float)
    created = Column(DateTime, nullable=False, server_default=func.now())
    updated = Column(DateTime, nullable=False, server_default=func.now(), onupdate=func.now())

class TempWeather(Base):
    __tablename__ = 'temp_weather'
    #__table_args__ = {'schema': 'solarman'}  # Specify the schema
    station_id = Column(BigInteger)
    device_id = Column(Integer, nullable=False, primary_key=True)
    device_sn = Column(String(25), nullable=False, primary_key=True)
    org_epoch = Column(Integer, nullable=True)
    epoch = Column(Integer, nullable=True)
    ts = Column(DateTime(timezone=True), nullable=False, primary_key=True)
    temp_c = Column(Float, nullable=True)
    panel_temp = Column(Float, nullable=True)
    humidity = Column(Float, nullable=True)
    rainfall = Column(Float, nullable=True)
    irr = Column(Float, nullable=True)
    daily_irr = Column(Float, nullable=True)

class Weather(Base):
    __tablename__ = 'weather'
    __table_args__ = (
        Index(None, 'device_sn'),
        Index(None, 'ts'),

        # Index('idx_weather_device_sn', 'device_sn'),
        # Index('idx_weather_ts', 'ts', postgresql_using='btree', postgresql_ops={'ts': 'desc'}),
        # {'schema': 'solarman'}  # Schema should come last
    )
    ts = Column(DateTime(timezone=True), nullable=False, primary_key=True)
    device_sn = Column(String(25), ForeignKey("solarman.devices.device_sn"), primary_key=True)
    station_id = Column(BigInteger, ForeignKey("solarman.stations.id"), nullable=False)
    device_id = Column(BigInteger)
    org_epoch = Column(Integer, nullable=False)
    epoch = Column(Integer, nullable=False)
    temp_c = Column(Float, nullable=True)
    panel_temp = Column(Float, nullable=True)
    humidity = Column(Float, nullable=True)
    rainfall = Column(Float, nullable=True)
    irr = Column(Float, nullable=True)
    daily_irr = Column(Float, nullable=True)
    created = Column(DateTime, nullable=False, server_default=func.now())
    updated = Column(DateTime, nullable=False, server_default=func.now(), onupdate=func.now())


INVERTERS = [
    {
        "site": "ABUJA",
        "ABUJA": [
            {"deviceSn": "2309184208", "deviceId": 240031597},
            {"deviceSn": "2309200109", "deviceId": 240030990},
            {"deviceSn": "2309198007", "deviceId": 240031585},
            {"deviceSn": "2309194017", "deviceId": 240031431},
            {"deviceSn": "2306296090", "deviceId": 240030831},
            {"deviceSn": "2309182126", "deviceId": 240031338},
            {"deviceSn": "2309200154", "deviceId": 240030917},
        ],
        "table": "fuel_kwh_abuja",
        "fn": "ABUJA_OFFICE_DG1_and_DG2_TANK.csv",
        "label": "BIOHENRY - UNHCR ABUJA OFFICE DG1 and DG2",
    },
    {
        "site": "OGOJA_GH",
        "OGOJA_GH": [
            {"deviceSn": "2309182179", "deviceId": 240480864},
            {"deviceSn": "2309198004", "deviceId": 240481013},
            {"deviceSn": "2309188195", "deviceId": 240481631},
            {"deviceSn": "2309208178", "deviceId": 240481437},
            {"deviceSn": "2309200145", "deviceId": 240481716},
        ],
        "table": "fuel_kwh_ogoja_gh",
        "fn": "OGOJA_GH_DG1_and_DG2_TANK.csv",
        "label": "BIOHENRY - UNHCR OGOJA GUEST HOUSE DG1 AND DG2",
    },
    {
        "site": "OGOJA",
        "OGOJA": [
            {"deviceSn": "2309194019", "deviceId": 240321506},  # not in current API
            {"deviceSn": "2408202575", "deviceId": 240897791},
            {"deviceSn": "2405052283", "deviceId": 240844835},
            {"deviceSn": "2309188295", "deviceId": 240295039},
            {"deviceSn": "2309188310", "deviceId": 240294993},
            {"deviceSn": "2309188199", "deviceId": 240321874},
        ],
        "table": "fuel_kwh_ogoja",
        "fn": "OGOJA_OFFICE_DG1_and_DG2_TANK.csv",
        "label": "BIOHENRY – UNHCR OGOJA OFFICE DG1 and DG2",
    },
    {
        "site": "LAGOS",
        "LAGOS": [
            {"deviceSn": "2401110046", "deviceId": 240033551},
            {"deviceSn": "2306296095", "deviceId": 240033630},
            {"deviceSn": "2306290070", "deviceId": 240033712},
        ],
        "table": "fuel_kwh_lagos_office",
        "fn": "LAGOS_OFFICE_DG1_and_DG2_TANK.csv",
        "label": "BIOHENRY -UNHCR LAGOS OFFICE DG1 and DG2",
    },
]

SITE_ID = [
    {"ABUJA": 63086751},
    {"OGOJA": 63122873},
    {"OGOJA_GH": 63151411},
    {"LAGOS": 63087453},
]

WEATHER = {
    "ABUJA": {"deviceSn": "002502255400-001", "deviceId": 240093462},
    "LAGOS": {"deviceSn": "002502325494-001", "deviceId": 240355934},
    "OGOJA": {"deviceSn": "002502705488-001", "deviceId": 240464333},
    "OGOJA_GH": {"deviceSn": "002502295492-001", "deviceId": 240482343},
}



from alembic.config import Config
from alembic.script import ScriptDirectory
from alembic import command
from sqlalchemy.schema import MetaData
import os
print(os.getcwd()) 

def check_db_schema(eng=db.set_local_defaultdb_engine(), alembic_cfg=None):
    err = None
    res = []
    try:
        metadata = MetaData()
        metadata.reflect(bind=eng)

        os.chdir("./unhcr_module/unhcr")
        # Load Alembic config
        if not alembic_cfg:
            alembic_cfg = Config("alembic.ini")
        script = ScriptDirectory.from_config(alembic_cfg)

        # Get latest Alembic revision
        head_revision = script.get_current_head()

        with Session(eng) as session:
            #head_revision = session.execute(text("SELECT version_num FROM solarman.alembic_version")).scalar()
            session.execute(text("SET search_path TO solarman, public;"))

            # Now you can perform queries, and the search path will be set for this session
            # result = session.execute(text("SELECT * FROM alembic_version"))  # It will look in 'solarman' first
            # print(result.fetchall())
            result = session.query(AlembicVersion).all()
            session.close()
        # Get current database revision
        conn = eng.connect()
        current_revision = conn.execute(text("SELECT version_num FROM solarman.alembic_version")).scalar()
        conn.close()

        res.append({
            "current_revision": current_revision,
            "head_revision": head_revision,
        })
        print(f"Database revision: {current_revision}")
        print(f"Latest Alembic revision: {head_revision}")

        if current_revision == head_revision:
            print("✅ Database schema is up-to-date.")
        else:
            print("⚠️ Database schema is out-of-date! Run 'alembic upgrade head'.")

    except Exception as e:
        print(f"Error: {e}")
        err = e
    finally:
        os.chdir("../../")
    return res, err


def migration_exists(msg, migration_dir):
    #timestamp = datetime.now().strftime("%Y%m%d%H%M%S")  # Use a timestamp to avoid conflicts
    migration_file_name = (msg[:20].lower()).replace(" ", "_") #f"{timestamp}_{msg}.py"
    for f in os.listdir(migration_dir):
        if migration_file_name in f:
            return True
    return False
    #return any(migration_file_name in f for f in os.listdir(migration_dir))


def create_migration(msg, versions, eng=db.set_local_defaultdb_engine(), sql=True):
    err = None
    res = []
    try:
        os.chdir("./unhcr_module/unhcr")
        # Load Alembic config
        alembic_cfg = Config("alembic.ini")
        migration_dir = os.path.join(os.getcwd(), "migrations", "versions")

        engine = create_engine(alembic_cfg.get_main_option("sqlalchemy.url"))

        inspector = inspect(engine)
        print("Tables in DB:", inspector.get_table_names(schema="solarman"))

        print('&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&')
        metadata = MetaData()
        metadata.reflect(engine, schema="solarman")

        print("Models found in DB:", metadata.tables.keys())
        print('**************************************************')

        from alembic.migration import MigrationContext
        conn = engine.connect()
        context = MigrationContext.configure(conn)
        db_metadata = MetaData()
        db_metadata.reflect(bind=engine, schema="solarman")

        from alembic.autogenerate import compare_metadata
        diffs = compare_metadata(context, Base.metadata)
        print("Schema Differences Found:", diffs)

        if diffs:
           #######migr = command.revision(alembic_cfg, message="Fix schema", autogenerate=True)
            # just run the migration
            if migration_exists(msg, migration_dir):
                # # capture the output of migration
                # # Get the original custom logger and store its handlers
                # original_logger = logging.getLogger('my_custom_logger')

                # # Save the original handlers and log level to restore later
                # original_handlers = original_logger.handlers
                # original_level = original_logger.level

                # # Create a new custom logger
                # logger = logging.getLogger('alembic')
                # logger.setLevel(logging.INFO)
                # log_stream = io.StringIO()
                # handler = logging.StreamHandler(log_stream)
                # logger.addHandler(handler)

                # command.upgrade(alembic_cfg, "head", sql=True)
                # sql = log_stream.getvalue()
                
                import tempfile
                import re

                # Backup original stdout
                original_stdout = sys.stdout

                # Create a temporary file to capture output
                temp_output = tempfile.TemporaryFile(mode="w+", encoding="utf-8")
                sys.stdout = temp_output  # Redirect stdout

                try:
                    # Run the function that logs output
                    command.upgrade(alembic_cfg, "head", sql=True)

                except Exception as e:
                    print("Error occurred:", e)

                finally:
                    # Restore stdout
                    sys.stdout = original_stdout
                    temp_output.seek(0)  # Move cursor to the beginning
                    sql = temp_output.read()
                    temp_output.close()

                
                

                lines = sql.split("\n")
                ver = versions[0]['head_revision']
                found = False
                sql = """"""
                for line in lines:
                    line = re.sub(r"[\r\n\t]", "", line)
                    if ver in line:
                        found = True
                        continue
                    if found:
                        if re.search(r'ALTER TABLE\s+(?!solarman\.)', line, re.IGNORECASE):
                            line = re.sub(r'ALTER TABLE\s+(\w+)', r'ALTER TABLE solarman.\1', line, flags=re.IGNORECASE) + "\n"
                        if re.search(r'DROP INDEX', line, re.IGNORECASE):
                            line = re.sub(r'DROP INDEX', r'DROP INDEX IF EXISTS', line, flags=re.IGNORECASE)
                            line = re.sub(r'(\S+)(;)', r"solarman.\1\2", line) + "\n"
                        if re.search(r'CREATE INDEX', line, re.IGNORECASE):
                            index_name = line.split()[2]
                            prefix = f'DROP INDEX IF EXISTS solarman.{index_name};\n' + "\n"
                            line = re.sub(r'CREATE INDEX', r'CREATE INDEX IF NOT EXISTS', line, flags=re.IGNORECASE) + "\n"
                            line = prefix + line
                        if re.search(r'DROP TABLE', line, re.IGNORECASE):
                                line = re.sub(r'DROP TABLE', r'DROP TABLE IF EXISTS', line, flags=re.IGNORECASE)
                                last_word = line.split()[-1]
                                # Check if the last word includes 'solarman.'
                                if not last_word.startswith('solarman.'):
                                    line = '-- ' + line.replace(last_word, f"solarman.{last_word}") + "\n"
                        if re.search(r'DROP CONSTRAINT', line, re.IGNORECASE):
                                 line = re.sub(r'DROP CONSTRAINT', r'DROP CONSTRAINT IF EXISTS', line, flags=re.IGNORECASE) + "\n"
                        if re.search(r'CREATE TABLE\s+(?!solarman\.)', line, re.IGNORECASE):
                                line = re.sub(r'CREATE TABLE\s+(\w+)', r'CREATE TABLE IF NOT EXISTS solarman.\1', line, flags=re.IGNORECASE) + "\n"
                        if re.search(r'ADD CONSTRAINT', line, re.IGNORECASE):
                                suffix = re.search(r"ADD CONSTRAINT (\S+)", line)
                                prefix = re.match(r"^(.*) ADD CONSTRAINT", line)
                                drop = f"{prefix.group(0).replace('ADD CONSTRAINT', '')} DROP CONSTRAINT IF EXISTS {suffix.group(1)};\n"
                                line = drop + line + "\n"
                        if line != "":
                            if 'COMMIT' in line:
                                continue
                            sql += line ##########re.sub(r'\s+', ' ', line).strip() +'\n'
                            # if 'fk_device_site_history_station_id' in line:
                            #     pass
                            # print(line)
                
                print(sql)
                #####sql += f" SELECT version_num FROM solarman.alembic_version"
                res.append({"sql": sql})
                ######db_res, err = db.sql_execute(sql, eng)
                # print('\n\n!!!!!!!!!!!!!!!!!!!!!!\n\n')
                # for q in sql.split("\n"):
                #     print(q)
                #     print('!!!!!!!!!!!!!!!!!!!!!!!!')
                
                
                with Session(eng) as session:
                    session.execute(text(sql))
                    session.commit()
                    session.begin()
                    session.execute(text(f"UPDATE solarman.alembic_version SET version_num = '{ver}';"))
                    session.commit()
                    # Now fetch the updated version
                    sm_res = session.execute(text("SELECT version_num FROM solarman.alembic_version")).scalar()
                session.close()
                if sm_res != ver:
                    err = f"Error: Version mismatch after migration. Expected: {ver}, Got: {sm_res}"
                
                ####x, err = db.sql_execute("SELECT version_num FROM solarman.alembic_version", eng)
                pass
                #######command.upgrade(alembic_cfg, "head")
                # Restore the original custom logger by resetting handlers and level
                # original_logger.handlers = original_handlers
                # original_logger.setLevel(original_level)

                # # 4. Reset the logger class to the original custom logger's class
                # logging.setLoggerClass(original_logger.__class__)
                return sm_res, err

            # Run the revision to create the migration file with autogeneration
            migr = command.revision(alembic_cfg, message=msg, autogenerate=True)
            res.append({"migration": migr})
            pass
        else:
            print("❌ No schema differences detected. Skipping migration.")
    except Exception as e:
        print(f"Error: {e}")
        err = e
    finally:
        os.chdir("../../")
    return res, err

def create_solarman_migration(msg, eng=db.set_local_defaultdb_engine()):
    if not msg:
        return None, print("No message provided.")

    res, err = check_db_schema(eng)
    if not err:
        migr, err = create_migration(msg, res, eng)
        if not err:
            res.append({"migration": migr})
        pass
    return res, err





def db_update_device_history(site_id, eng, utc=datetime(2024, 10, 1, 0, 0, 0, tzinfo=timezone.utc)):
    site_devices = []
    for s in SITE_ID:
        if str(site_id) not in str(s):
            continue
        site_key = next(iter(s))
        print(site_key)
        site_devices.append(WEATHER[site_key])
        for i in INVERTERS:
            if i['site'] != site_key:
                continue
            site_devices.extend(i[site_key])
            break
        break
    print(site_devices)

    with Session(eng) as session:
        device_sns = [device['deviceSn'] for device in site_devices]
        print(device_sns)
        # Query all devices
        devices = session.scalars(select(Device).filter(Device.device_sn.in_(device_sns))).all()

        # Print results
        for device in devices:
            print(f"Device SN: {device.device_sn}, ID: {device.device_id}, Type: {device.device_type}")
        
    
            new_history = DeviceSiteHistory(
            station_id=site_id,  # Replace with actual station_id
            device_sn=device.device_sn,  # Replace with actual device_sn
            device_id=device.device_id,  # Replace with actual device_id
            comment="Site online around Oct 1st 2024",
            start_time=utc,  # Current time as start time
            end_time=None  # Still at this site
            )

            # Add to session and commit
            session.merge(new_history)
            session.commit()
    return devices, None        
    pass
