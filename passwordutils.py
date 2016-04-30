import argon2
import binascii
from random import SystemRandom


def generateSalt(minlen=50, maxlen=60):
    sysrand = SystemRandom()
    return ''.join(chr(sysrand.randint(32, 126))
                   for i in range(sysrand.randint(minlen, maxlen)))


def hashPassword(password, salt, algo='argon2', params={'t': 5}):
    if algo == 'argon2':
        return binascii.hexlify(
            argon2.argon2_hash(password,
                               salt,
                               **params)).decode()
    else:
        raise ValueError('Unknown algorithm "%s".' % algo)
