rsync -r -e ssh . virt@192.168.123.41:/home/virt/nvmevirt
ssh virt@192.168.123.41 "make -C /home/virt/nvmevirt clean && make -j -C /home/virt/nvmevirt"
scp virt@192.168.123.41:/home/virt/nvmevirt/nvmev.ko /home/move/kerneldebug/
