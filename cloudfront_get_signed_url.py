import datetime
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import padding
from botocore.signers import CloudFrontSigner


def rsa_signer(message):
#### .pem is the private keyfile downloaded from CloudFront keypair
    with open('pk-file.pem', 'rb') as key_file:
        private_key = serialization.load_pem_private_key(
        key_file.read(),
        password=None,
        backend=default_backend()
    )
    signer = private_key.signer(padding.PKCS1v15(), hashes.SHA1())
    signer.update(message)
    return signer.finalize()
key_id = 'file'
url = 'https://someurl.cloudfront.net/path/file'

current_time = datetime.datetime.utcnow()
expire_date = current_time + datetime.timedelta(minutes = 2)
cloudfront_signer = CloudFrontSigner(key_id, rsa_signer)
# Create a signed url that will be valid until the specfic expiry date
# provided using a canned policy.
signed_url = cloudfront_signer.generate_presigned_url(
url, date_less_than=expire_date)
print(signed_url)
