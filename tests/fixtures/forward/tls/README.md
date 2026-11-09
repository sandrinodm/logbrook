Synthetic RSA certificates and unencrypted private keys for the receiver tests. They
don't authenticate any deployment, and the CA's signing key is not checked in.
The server certificate's DNS SAN is `localhost`. The client certificate (CN
`synthetic-producer`) stands for a test producer; the receiver maps the SHA-256
fingerprint of its verified leaf certificate (DER) to a principal.
Generated with the system LibreSSL on 2026-11-04: RSA-2048 keys, SHA-256 signatures,
100-year validity and a self-signed CA made only for these tests.
