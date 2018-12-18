.. _query-with-arrays-and-maps:

Query With Arrays and Maps
##########################

KSQL enables using complex types, like arrays and maps, in your queries. You
use familiar syntax, like ``myarray ARRAY<type>`` and ``myarray[0]`` to declare
and access these types.

The following example shows how to create a KSQL table from a Kafka topic that
has array and map fields. Also, it shows how to run queries to access the array
and map data. It assumes a Kafka topic, named ``users``. To see this example in
action, create the ``users`` topic by following the procedure in
:ref:`ksql_quickstart-docker`.

.. important::

   When you start the ksql-datagen process for the ``users`` topic, set the
   ``quickstart`` parameter to ``users_``, to add the array and map fields to
   the ``user`` records. Be sure to append the ``_`` character. The 
   array and map fields are named ``interests`` and ``contactinfo`` and have
   type ARRAY<STRING> and MAP<STRING,STRING>.

Create a Table With Array and Map Fields
**************************************** 

Run the following query to create a KSQL table on the ``users`` topic. The
array and map fields are defined in the ``interests ARRAY<STRING>`` and
``contactinfo MAP<STRING,STRING>`` declarations.

.. code:: sql

    CREATE TABLE users \
      (registertime BIGINT, \
       userid VARCHAR, \
       gender VARCHAR, \
       regionid VARCHAR, \
       interests ARRAY<STRING>, \
       contactinfo MAP<STRING,STRING>) \
      WITH (KAFKA_TOPIC = 'users', \
            VALUE_FORMAT='JSON', \
            KEY = 'userid');

Your output should resemble:

::

    Message
   ---------------
    Table created
   ---------------

The table is ready for you to run queries against it.

Query a Table That Has Array and Map Fields
*******************************************

With the ``users`` table created, you can query the array field and the map
field. Run the following CREATE TABLE AS SELECT statement to create a
persistent query that has the first item in the user's ``interests`` array
and the user's city and zip code from the ``contactinfo`` map.

.. code:: sql

    CREATE TABLE users_interest_and_contactinfo AS \
      SELECT interests[0] AS first_interest, \
             contactinfo['zipcode'] AS zipcode, \
             contactinfo['city'] AS city, \
             userid, \
             gender, \
             regionid \
      FROM users;

Your output should resemble:

::

    Message
   ---------------------------
    Table created and running
   ---------------------------

Run the following SELECT query to view the table: 

.. code:: sql
    
    SELECT userid, first_interest, city, zipcode \
      FROM users_interest_and_contactinfo;

Your output should resemble:

::

    User_4 | Game | Palo Alto | 94301
    User_9 | Game | San Jose | 95112
    User_3 | News | San Mateo | 94403
    User_6 | Game | Irvine | 92617
    User_1 | Game | Irvine | 92617
    User_7 | News | San Mateo | 94403
    User_2 | News | Irvine | 92617
    User_8 | Game | San Mateo | 94403
    User_5 | Game | San Carlos | 94070
    ^CQuery terminated

Press Ctrl+C to terminate the query.

Next Steps
**********

* :ref:`create-a-table-with-ksql`
* :ref:`create-a-stream-with-ksql`
* :ref:`join-streams-and-tables`

