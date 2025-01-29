from joserfc.jwk import RSAKey

STEP_DEFINITIONS = {
    'preview_zip': "oarepo_file_pipeline_server.pipeline_steps.preview_zip.PreviewZip",
    'preview_picture': "oarepo_file_pipeline_server.pipeline_steps.preview_picture.PreviewPicture",
    'create_zip': "oarepo_file_pipeline_server.pipeline_steps.create_zip.CreateZip",
    'extract_directory_zip': "oarepo_file_pipeline_server.pipeline_steps.extract_directory_zip.ExtractDirectoryZip",
    'extract_file_zip': "oarepo_file_pipeline_server.pipeline_steps.extract_file_zip.ExtractFileZip",
    'crypt4gh': "oarepo_file_pipeline_server.pipeline_steps.crypt4gh.Crypt4GH"
}

# KEYS used only for testing
server_private_key="""
-----BEGIN PRIVATE KEY-----
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

repo_public_key="""
-----BEGIN PUBLIC KEY-----
MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAtglGihFRl+cD43AKsKEA
g30vVS8b5azZVLT/myENye+1RqBm/VF4YB/gIwwBPmAQxSL/SbIuxcEEDuguqkpA
BrqIfy6/MimeQLfe4xz1smOrq4NE+BkadlpZ24KwclubOsgE/hnG/XIk+WElVbzO
3fza77nIbieGZez5f67v6rgnWIMPWoC7P+sRUN6R5Td9jFBKK8epnKJlD2uvXS6Y
jPSmU05R0mJvAl8z3TKLN7l6vuRA/rtLK/IfmFYUiVrpQdht74GtwzSo3KSvUApv
gQA2C9QuD+gWW2W7dfsOLpGgWF9v3EMCvRWUHs7dGHIxVDT5NpZtJTRVJ5n04Wug
twIDAQAB
-----END PUBLIC KEY-----
"""
repo_public_key = RSAKey.import_key(repo_public_key)
server_private_key = RSAKey.import_key(server_private_key)


INVENIO_S3_PROTOCOL="http"
INVENIO_S3_HOST="127.0.0.1"
INVENIO_S3_PORT=9000
INVENIO_S3_PORT1=9001
# must be at least 3 characters
INVENIO_S3_ACCESS_KEY="aa-nr_docs-aa"

# must be at least 8 characters
INVENIO_S3_SECRET_KEY="aaa-nr_docs-aaa"

# USED ONLY FOR TESTING
server_key_priv_c4gh = """
-----BEGIN CRYPT4GH PRIVATE KEY-----
YzRnaC12MQAEbm9uZQAEbm9uZQAg5aLYHVFzZxtzr0UqQDBwyQBu7jUYsC/bkFR5TnVjSaQ=
-----END CRYPT4GH PRIVATE KEY-----
"""

server_key_pub_c4gh = """
-----BEGIN CRYPT4GH PUBLIC KEY-----
bzoBg9BgjnAkesJ5pDiSoHaObr7GNi627advrQ8oaGM=
-----END CRYPT4GH PUBLIC KEY-----
"""