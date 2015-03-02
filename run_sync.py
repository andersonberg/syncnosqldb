__author__ = 'aberg'

import sys
from sync import Sincronizador
import time


def main():
    periodo = sys.argv[1]
    sync = Sincronizador()
    while True:
        if periodo >= 0:
            sync.sync()
            time.sleep(periodo)


if __name__ == "__main__":
    main()
