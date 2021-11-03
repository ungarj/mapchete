=========
Run tests
=========

.. code-block:: shell

    pytest -v --cov mapchete


Under certain environments, curl-based tests can fail. In this case, try:

.. code-block:: shell

    export CURL_CA_BUNDLE=/etc/ssl/certs/ca-certificates.crt
