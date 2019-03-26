.. _ksql_quickstart-ccloud:

Writing Streaming Queries Against |ak-tm| Using KSQL and |ccloud|
#################################################################

Prerequisites
    - `Access to Confluent Cloud <https://www.confluent.io/confluent-cloud/>`__
    - :ref:`cloud-limits`

.. include:: ../../../quickstart/includes/docker-prereqs.rst



#. Set up your |ccloud| account.
#. Create an |ak| cluster.
#. Configure |sr-ccloud|. 
#. Install the |ccloud| CLI.
#. Install |cp| locally.
#. Produce data to your |ak| cluster in |ccloud|.
#. Write streaming queries by using the KSQL Editor in |ccloud|.  

Create an |ak-tm| Cluster in |ccloud|  
*************************************

Follow steps 1 through 3 in :ref:`cloud-quickstart`.

TODO: Add anchors for these in docs/quickstart/cloud-quickstart.rst.

Step 1: Create Kafka Cluster in Confluent Cloud
Step 2: Install and Configure the Confluent Cloud CLI
Step 3: Configure |sr-ccloud|. For this demo, you don't need to set up the 
Java client.

Install |cp| locally
********************

.. steps taken from docs/tutorials/examples/ccloud/docs/index.rst

#. Clone the `examples GitHub repository <https://github.com/confluentinc/examples>`__.

   .. code:: bash

     $ git clone https://github.com/confluentinc/examples

#. Change directory to the |ccloud| demo.

   .. code:: bash

     $ cd examples/ccloud

#. Start the demo by running a single command. You have two choices: using a
   |cp| local install or Docker Compose. This will take less than 5 minutes
   to complete.

   .. sourcecode:: bash

      # For Confluent Platform local install using Confluent CLI
      $ ./start.sh

      # For Docker Compose
      $ ./start-docker.sh

Write Streaming Queries
***********************

.. as with the other "basics" topics

Next Steps
**********