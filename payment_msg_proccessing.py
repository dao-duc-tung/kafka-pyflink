from pyflink.datastream import StreamExecutionEnvironment, TimeCharacteristic
from pyflink.table import StreamTableEnvironment, DataTypes, EnvironmentSettings
from pyflink.table.expressions import call, col
from pyflink.table.udf import udf
provinces = ("Beijing", "Shanghai", "Hangzhou", "Shenzhen", "Jiangxi", "Chongqing", "Xizang")
@udf(input_types=[DataTypes.STRING()], result_type=DataTypes.STRING())
def province_id_to_name(id):
    return provinces[id]
def log_processing():
    env = StreamExecutionEnvironment.get_execution_environment()
    t_env = StreamTableEnvironment.create(stream_execution_environment=env)
    t_env.get_config().get_configuration().set_boolean("python.fn-execution.memory.managed", True)
    create_kafka_source_ddl = """
            CREATE TABLE payment_msg(
                createTime VARCHAR,
                orderId BIGINT,
                payAmount DOUBLE,
                payPlatform INT,
                provinceId INT
            ) WITH (
                'connector' = 'kafka',
                'topic' = 'payment_msg',
                'properties.bootstrap.servers' = 'kafka:9092',
                'properties.group.id' = 'test_3',
                'scan.startup.mode' = 'latest-offset',
                'format' = 'json'
            )
            """
    create_kafka_source_ddl_2 = """
            CREATE TABLE payment_msg_2(
                createTime VARCHAR,
                orderId BIGINT,
                payAmount DOUBLE,
                payPlatform INT,
                provinceId INT
            ) WITH (
                'connector' = 'kafka',
                'topic' = 'payment_msg',
                'properties.bootstrap.servers' = 'kafka:9092',
                'properties.group.id' = 'test_3',
                'scan.startup.mode' = 'latest-offset',
                'format' = 'json'
            )
            """
    create_kafka_sink_ddl = """
            CREATE TABLE payment_msg_3(
                orderId BIGINT,
                payAmount_1 DOUBLE,
                payAmount_2 DOUBLE
            ) WITH (
                'connector' = 'kafka',
                'topic' = 'payment_msg_3',
                'properties.bootstrap.servers' = 'kafka:9092',
                'properties.group.id' = 'test_3',
                'scan.startup.mode' = 'latest-offset',
                'format' = 'json'
            )
            """
    create_join_ddl = """
            INSERT INTO payment_msg_3
            SELECT a.orderId, a.payAmount, b.payAmount
            FROM payment_msg as a, payment_msg_2 as b
            WHERE a.orderId = b.orderId
            """
    t_env.execute_sql(create_kafka_source_ddl)
    t_env.execute_sql(create_kafka_source_ddl_2)
    t_env.execute_sql(create_kafka_sink_ddl)
    t_env.execute_sql(create_join_ddl)

if __name__ == '__main__':
    log_processing()
