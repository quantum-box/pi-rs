These public test-only certificates and private key are synthetic fixtures for
loopback TLS tests. They are not used outside the test server. The leaf
certificate is valid only for the IP address 127.0.0.1. Tests verify a trusted
connection and reject both a different hostname and an unrelated CA.
