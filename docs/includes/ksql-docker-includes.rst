.. docker_prereqs_start

**Prerequisites**

-  `Git <https://git-scm.com/downloads>`__
-  Docker must be installed and configured with at least 8 GB of memory.
     -  `Docker for Mac <https://docs.docker.com/docker-for-mac/install/>`__
     -  `All platforms <https://docs.docker.com/engine/installation/>`__

   Check the available memory for Docker by using the ``docker system info``
   command:

   .. code:: bash

      docker system info | grep -i memory

   Your output should resemble:

   .. code:: bash

      Total Memory: 7.768GiB

.. docker_prereqs_end