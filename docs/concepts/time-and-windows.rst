.. _time-and-windows:

Time and Windows
################

During the life cycle of a record, it experiences a number of events
which occur at particular times. Three important times in a record's 
life cycle are *Event-time*, *Ingestion-time*, and *Processing-time*. 

Representing time consistently enables aggregation operations on streams,
like SUM, that have time boundaries.

KSQL supports the following notions of time:

Event-time
    The time when a record is created by the data source. Achieving event-time
    semantics requires embedding timestamps in records when an event occurs and
    the record is produced.

    For example, if the event is a geo-location change reported by a GPS sensor
    in a car, the associated event-time is the time when the GPS sensor captured
    the location change.

Ingestion-time
    The time when a record is stored in a topic partition by a Kafka broker.
    Ingestion-time is similar to event-time, as a timestamp gets embedded in the
    record, but the ingestion timestamp is generated when the Kafka broker appends
    the record to the target topic.
    
    Ingestion-time may approximate event-time reasonably well if the time
    difference between creation of the record and its ingestion into Kafka
    is sufficiently small. This means that ingestion-time may be an alternative
    for use cases where event-time semantics aren't possible.
    
    You may face this situation when data producers don't embed timestamps in
    records, as in older versions of Kafka’s Java producer client, or when the
    producer can't assign timestamps directly, like when it doesn't have access
    to a local clock.

Processing-time
    The time when the record is consumed by a stream processing application.
    The processing-time happen immediately after Ingestion-time, or it may
    happen much later, in milliseconds, hours, or days.

    For example: imagine an analytics application that reads and processes the
    geo-location data reported from car sensors, and presents it to a
    fleet-management dashboard. In this case, processing-time in the analytics
    application might be milliseconds or seconds after event-time, like for
    real-time pipelines based on Apache Kafka and Kafka Streams) or it might be
    hours, like for batch pipelines based on Apache Hadoop or Apache Spark.

KSQL assigns a timestamp to every data record by using *timestamp extractors*,
which are per-record timestamps that describe the progress of a stream over time.
Timestamps are used by time-dependent operations, like joins. Records may be
out-of-order within the stream.

Concrete implementations of timestamp extractors may retrieve or compute timestamps
based on the actual contents of data records such as an embedded timestamp field to provide
event-time or ingestion-time semantics, or use any other approach such as returning the
current wall-clock time at the time of processing, thereby yielding processing-time semantics
to stream processing applications.

Developers can thus enforce different notions/semantics of time depending on their business needs.

Finally, whenever a KSQL application writes records to Kafka,
then it will also assign timestamps to these new records.
The way the timestamps are assigned depends on the context:

* When new output records are generated via directly processing some input record,
  output record timestamps are inherited from input record timestamps directly.
* When new output records are generated via periodic functions, the output record
  timestamp is defined as the current internal time of the stream task.
* For aggregations, the timestamp of the resulting update record will be that of
  the latest input record that triggered the update.

.. tip::

Know your time: When working with time you should also make sure that additional
aspects of time such as time zones and calendars are correctly synchronized – or
at least understood and traced – throughout your stream data pipelines. It often
helps, for example, to agree on specifying time information in UTC or in Unix time
(such as seconds since the epoch). You should also not mix topics with different time semantics.


Windowing
*********

Windowing lets you control how to group records that have the same key for stateful
operations such as aggregations or joins into so-called windows. Windows are tracked
per record key.

When working with windows, you can specify a retention period for the window.
This retention period controls how long KSQL waits for out-of-order or late-arriving records
for a given window. If a record arrives after the retention period of a window has passed,
the record is discarded and will not be processed in that window.

We added a new configuration that we call "grace period" that determines how long
we wait before we close a window. Retention time is still a valid parameter that defines
how long we store the (potentially) closed window -- we do this to allow to access the
window via Interactive Queries even if it's already closed.

Late-arriving records are always possible in the real world and should be properly accounted
for in your applications. It depends on the effective time semantics how late records are handled.
In the case of processing-time, the semantics are “when the record is being processed”, which means
that the notion of late records is not applicable as, by definition, no record can be late.
Hence, late-arriving records can only be considered as such (i.e. as arriving “late”) for
event-time or ingestion-time semantics. In both cases, KSQL is able to properly
handle late-arriving records.

.. image:: ../img/ksql-window.png


.. image:: ../img/ksql-window-aggregation.png


Session Window start and end time are both inclusive (in contrast to time-windows),
and there is always a record in the session window with the start and end timestamps
(because the timestamp of the first and last record in the window define window start and end time).
