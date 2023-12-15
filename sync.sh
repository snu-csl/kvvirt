rsync -r -e ssh . carl@192.168.122.8:/home/carl/nvmevirt
ssh carl@192.168.122.8 "make -C /home/carl/nvmevirt clean && make -j -C /home/carl/nvmevirt"
scp carl@192.168.122.8:/home/carl/nvmevirt/nvmev.ko /home/cduffy/kerneldebug/
