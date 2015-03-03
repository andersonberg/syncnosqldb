__author__ = 'aberg'

import sys
from sync import Sincronizador
import time


def main():
    periodo = int(sys.argv[1])
    sync = Sincronizador()
    count = 0
    while True:
        if periodo >= 0:
            sync.sync()
            count += 1
            print("Passo: %s" % count)
            time.sleep(periodo)


if __name__ == "__main__":
    main()
