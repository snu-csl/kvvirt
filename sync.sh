rsync --exclude='*log*' -r -e ssh . virt@192.168.123.41:/home/virt/nvmevirt
ssh virt@192.168.123.41 "cd /home/virt/nvmevirt; ./build.sh rel"
scp virt@192.168.123.41:/home/virt/nvmevirt/nvmev.ko /home/move/kerneldebug/
