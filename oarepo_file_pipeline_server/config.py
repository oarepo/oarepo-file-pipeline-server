#
# Copyright (c) 2025 CESNET z.s.p.o.
#
# This file is a part of oarepo_file_pipeline_server (see https://github.com/oarepo/oarepo-file-pipeline-server).
#
# oarepo_file_pipeline_server is free software; you can redistribute it and/or modify it
# under the terms of the MIT License; see LICENSE file for more details.
#
"""Configuration for the Oarepo File Pipeline Server."""

from __future__ import annotations

from joserfc.jwk import RSAKey

STEP_DEFINITIONS = {
    "crypt4gh": "oarepo_file_pipeline_server.pipeline_steps.crypt4gh.Crypt4GHDecryptStep",
}

"""Default algorithms"""
PIPELINE_SIGNING_ALGORITHM = "RS256"
PIPELINE_ENCRYPTION_ALGORITHM = "RSA-OAEP"
PIPELINE_ENCRYPTION_METHOD = "A256GCM"

# KEYS used only for testing
repo_private_key = """-----BEGIN PRIVATE KEY-----
MIIEvQIBADANBgkqhkiG9w0BAQEFAASCBKcwggSjAgEAAoIBAQC2CUaKEVGX5wPj
cAqwoQCDfS9VLxvlrNlUtP+bIQ3J77VGoGb9UXhgH+AjDAE+YBDFIv9Jsi7FwQQO
6C6qSkAGuoh/Lr8yKZ5At97jHPWyY6urg0T4GRp2WlnbgrByW5s6yAT+Gcb9ciT5
YSVVvM7d/NrvuchuJ4Zl7Pl/ru/quCdYgw9agLs/6xFQ3pHlN32MUEorx6mcomUP
a69dLpiM9KZTTlHSYm8CXzPdMos3uXq+5ED+u0sr8h+YVhSJWulB2G3vga3DNKjc
pK9QCm+BADYL1C4P6BZbZbt1+w4ukaBYX2/cQwK9FZQezt0YcjFUNPk2lm0lNFUn
mfTha6C3AgMBAAECggEAECHiTnoacQvYHF/hkqWyFfUSLMpv/nrDB+7CeE0Fm8/S
kN/GQMznjh1FD9YQhiadVds0JKPV4VCpu2h4Oj86TV5ammraJufpTnL9HcODQrvy
itCnXBVdcv+u1vVODdRwfVUcnChMqkljXXZuiJqi+qld/cDJMnnuPzoxIk+uk254
KMFV9bmcM5npxH/B1383E+rU/v/YW35ms8MZuleo32xMKY5inHasvpMCIHccvrvi
+NIOE66FSMOJPqdJVexkV9Kmi3/29py0jtjt6XqrJuD+pYUQIrOYSSeCx6F8GqDe
XR/rEVSTcCcgipz71vdSzAUK2LFUhzPaipwjkUeWgQKBgQDqrUKtLnmzwTSRZoPe
USAlcJaKfi3pOAyko9joCyEfwH8grQNG5saBMitO3ClmzlxL6qgqzMXM+xyJ8KFx
L0HXADxfOEZGOCRx/bzDaW2YbPQYAjQVOo3wcGkkcJpdwDSL/hpWfNgJGzKlJfO3
2IPEWHzO5f6mhjcESUP6R0InXQKBgQDGk49tJP57RrrY2E26T6MLGYaUObqSwhVN
Xyb9B/Ee6/d60CRseD8jLgJ/TBaqJfx9m5grGWz8z9QV6UTb5MbPx0u7lGN+hquU
KFxtjskQ1tyfg3emPkSBvdxUD8Sq9ebot2/B3mAvuhqdyx/5WeK2LiaVnDixFoKH
PT9947hLIwKBgHzueeWKLV4Fh/+z7JXI6G0mD+5wl+5lWU24sDtv2VV2+/agRHNV
Xe3fkHCuAhhp2XbM2HPYiaDDOgExKjEAMHPN+1XRto+hSb2pj/kTwjV4I0Y4vhNj
FbcfkMnGbFdmgFLalpjeY4ANi5uhpaqEyDkZxm+6vyNVpipQ+rBdiRk9AoGAQcct
cn0XoyRJznzQOpAYtRuOfdklmWma/tcvJhAUaibGArOh7SBj4bZi82Hz/Aa7Paxl
2pkAhjodyehMe/6rcLZWutsrngTkHx7DhzMOHXre+CPnZXUo4kVPD7VtcygjhiEF
bxXHjOe721smy0VgGPLuqw5lpRuMv1mlh4EAUjsCgYEAmGKkvoV2k0P/X1IxiA/d
CP9pQ3A7d8jXIq9F9tbFIg90FTPvpCSUPuPDafTnV6ODJ77Zp3GMIGQld19ausKF
JLtzz8CQoxhIp5d0UlL60DfDiA2pXr2NRx50etOVrwIkvv+5tSvbefjsoyaUQWhD
5h6tAQKsmxV7MrJLU7qnV24=
-----END PRIVATE KEY-----
"""

repo_public_key = """-----BEGIN PUBLIC KEY-----
MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAtglGihFRl+cD43AKsKEA
g30vVS8b5azZVLT/myENye+1RqBm/VF4YB/gIwwBPmAQxSL/SbIuxcEEDuguqkpA
BrqIfy6/MimeQLfe4xz1smOrq4NE+BkadlpZ24KwclubOsgE/hnG/XIk+WElVbzO
3fza77nIbieGZez5f67v6rgnWIMPWoC7P+sRUN6R5Td9jFBKK8epnKJlD2uvXS6Y
jPSmU05R0mJvAl8z3TKLN7l6vuRA/rtLK/IfmFYUiVrpQdht74GtwzSo3KSvUApv
gQA2C9QuD+gWW2W7dfsOLpGgWF9v3EMCvRWUHs7dGHIxVDT5NpZtJTRVJ5n04Wug
twIDAQAB
-----END PUBLIC KEY-----
"""

server_public_key = """-----BEGIN PUBLIC KEY-----
MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAs8vm6OFyOpPyP6nxQwNB
pX19IKf5SMNq4FEADK/zWobLkfEOlVMhQ77/7LsA822PO/K3LHtoA42zz+CXmDir
hLu6R1j1i8/C8Z98bJ9pVigkMhD0F8B6L04FoRnN8ycj3FYfmxu3QRqjg+nF+5cN
B8Do0vVFw+IOcca9LJbqHNj59CQmJpuRO5T4l0mNmGjdTnCyG/YQdLlV1hvw85Zp
UCcUbrlVdC9b3wJ1IhgZ6RCEE4sjcuY2XMsV4bf+9uwKHa6OVwNXdX6hLVOvCbBW
GrqMHOhsJ8Sf7j1sL8LeplSjiGmqJfl1tLR7M4zr72Vt1JoYDxMWWfaZs0pwVT8o
ywIDAQAB
-----END PUBLIC KEY-----
"""

server_private_key = """-----BEGIN PRIVATE KEY-----
MIIEvQIBADANBgkqhkiG9w0BAQEFAASCBKcwggSjAgEAAoIBAQCzy+bo4XI6k/I/
qfFDA0GlfX0gp/lIw2rgUQAMr/NahsuR8Q6VUyFDvv/suwDzbY878rcse2gDjbPP
4JeYOKuEu7pHWPWLz8Lxn3xsn2lWKCQyEPQXwHovTgWhGc3zJyPcVh+bG7dBGqOD
6cX7lw0HwOjS9UXD4g5xxr0sluoc2Pn0JCYmm5E7lPiXSY2YaN1OcLIb9hB0uVXW
G/DzlmlQJxRuuVV0L1vfAnUiGBnpEIQTiyNy5jZcyxXht/727Aodro5XA1d1fqEt
U68JsFYauowc6GwnxJ/uPWwvwt6mVKOIaaol+XW0tHszjOvvZW3UmhgPExZZ9pmz
SnBVPyjLAgMBAAECggEAUImjLykhnmy8JFlvGXoBc2xxWunzR+1FWCLgd05vn1rn
IEIPKsN4kJyjjjq8M86dTRithY7n6kOUyqbLsSOdbREcYa5PG2ge5lXvCccki7Pi
dszSUjtlYAA+lEn3T5Z2QVIQyU2SembA3SugBFFGxHTctfapYBPILZ39Cla1muK0
TaV3QeAqNC/ikIa6dHzA+BsSKawczHeIA2D+9s5OsASuBbukn9pw6yXDG8DcI73Z
uhbsnoZEu4Ml0HegzObvozqb6EZwlwMQbVarDuVA3Jop2X6ytgyUd6aX0D0jA9MW
0rqlM3+x8TRtgkNm1uzB7w5vRwNLSIjH1ahMWm8ZYQKBgQDeM6Ua7WRHc+SY+ctN
TXbjCpRYIqdSRbKv6++m4fikZqkTf6Fb+hWh8T3jQpo8lAjQt+mLhtxiXNI3JTQu
69ksdKRBV9pvBsWTcFn4Jlu1fCQLk2Hf98En/dX5eFyV5fWWfJm3uIZye6akJSfq
rRmzyobJZbFj2BDU+vup3jP+4QKBgQDPJQf/kw70qn8nSEQrT0OsxubRZJ9jqBwp
VgosFMVMexAQvYweQ5EmS9ZiIhSvQLP0ZSTTIAsbl4DC535qsdX/Sf6eXh4OfRsV
m/NU/PCLsRr8qolDIEH0TGmQKGuoeJoDNyp8q6lRvfnFyKmrGCdYtDuryGHJSVu9
LEmlx2t5KwKBgFd5bV4UZo3aifvPGsHr5QmseInZ2pUA6z9mWooQG5pc7+LFM/jJ
kwqVtg9pgN6oSHAidsZ+6POwJvGeq9Rs9KoToTY4J73dpJpOeJzAPQpNPMNx2e4Z
0uizfTEguRIp3WzI0JsLAaLAGvIzzmsMijnFWRqf9h2gScAOrlRJLZ8BAoGBAMtS
xe8PIfb2A6lDPeZk/0BwW8/cvLbNJBdO5N0v5hmUEcjcxNRP7gFxHxVj7nm3QOv6
+5JgOYbzxueI4oVH2Y2jy9EXANmn4xXq5YXeYR480QiBPAovd42cE2H0yveqqUHO
vF1zAdfCaZDBzgiqxLRE9O1A2vsAjpO5DPE0NUHRAoGAP0siJ4Wk2XDCFcNM3fzK
FXcK3FiHdSWkTelbFU60kOpXrEpHsWShpVM0d/LtbmYPB4gtFfXCjMHf80F/PZrr
Zt4sAc6TAS+xNfT7djzy8N9tvjd1220orFLZUr1VC+m0+jfPM7dzJ9MVn3386Skm
oXMkXQNjJhyifeoAmStK3G4=
-----END PRIVATE KEY-----
"""
repo_public_key = RSAKey.import_key(repo_public_key)
repo_private_key = RSAKey.import_key(repo_private_key)
server_public_key = RSAKey.import_key(server_public_key)
server_private_key = RSAKey.import_key(server_private_key)


# USED ONLY FOR TESTING
server_key_priv_c4gh = """
-----BEGIN CRYPT4GH PRIVATE KEY-----
YzRnaC12MQAEbm9uZQAEbm9uZQAgL2Pclwv7OQ1Ekz3Uvsu1TlFdte8y/ObE8GstM0HUMV8=
-----END CRYPT4GH PRIVATE KEY-----
"""

server_key_pub_c4gh = """
-----BEGIN CRYPT4GH PUBLIC KEY-----
0iPOxp/rF7w3R2A4ElxhigHmTt2WCh9HxMcRDoDl7F4=
-----END CRYPT4GH PUBLIC KEY-----
"""
