# Проект 8-го спринта

### Описание файла src/subscribers_feedback.sql
В данном файле представлено описание таблицы public.subscribers_feedback, в которую будут сохраняться данные результирующего DataFrame.

### Описание файла src/config.py
1. src_topic - топик в Kafka, из которого забираются данные об акциях ресторанов.

2. dst_topic - топик в Kafka, в который нужно сохранить данные после вычислений.

3. spark_jars_packages - список JAR-файлов, используемых данным скриптом во время работы. 

4. pg_settings - словарь данных, состоящий из:
     - src - данные подключения к СУБД PostgreSQL для забора информации о подписчиках на те или иные рестораны.
     - dst - данные подключения к СУБД PostgreSQL для сохранения данных вычисленного DataFrame.

5. kafka_options - словарь с опциями безопасного подключения к kafka-брокеру.

### Описание файла src/main.py
1. create_spark_session() - функция для создания SparkSession.

2. get_user_schema() - функция возврата StructType для декодирования value из kafka.

3. read_kafka_stream() - функция чтения данных об акциях ресторанов из топика kafka.

   Поскольку в kafka данные хранятся в двоичном виде, то значения value нужно преобразовать в строку.
   
   Поскольку value представляет собой сообщение типа JSON, то нужно воспользоваться функцией from_json для получения данных полей.
   
4. filter_stream_data() - фильтрация данных и очистка данных от дубликатов.

   Естественный ключ - комбинация полей restaurant_id, adv_campaign_id, adv_campaign_datetime_start, на её основе убираются дубли.
   
   В качестве временной watermark используется поле datetime_created, максимальная разница во времени 10 минут.
   
   Из полученного набора данных нужно оставить те строки, у которых текущая дата и время находится в отрезке от adv_campaign_datetime_start до adv_campaign_datetime_end

5. read_subscribers_data() - функция забора данных о подписчиках на рестораны из СУБД PostgreSQL.

6. join_and_transform_data() - функция соединения данных об акциях ресторанов и подписчиках на рестораны.

   Соединение происходит по полю restaurant_id, тип inner.

7. write_to_postgresql() - функция записи данных в СУБД PostgreSQL.

8. write_to_kafka() - функция записи данных в Kafka.

9. save_to_postgresql_and_kafka() - функция сохранения полученного DataFrame в СУБД PostgreSQL и topic Kafka.

   Перед сохранением нужно выполнить метод persist() объекта DataFrame для помещения данных в памяти.
   
   Затем идёт сохранение данныз в СУБД PostgreSQL и topic Kafka.
   
   Потом память очищается от DataFrame.
   
10. main() - главная функция скрипта.
