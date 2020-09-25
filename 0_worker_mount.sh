
# mkdir our mount dir
sudo mkdir -p /root/keti1-ruc/disks

# list all block devices
lsblk -o NAME,FSTYPE,SIZE,MOUNTPOINT,LABEL

# list all devices' partitions table
fdisk -l

# format and mount the avalible patition in /root/keti1-ruc/UUID/
cd /root/keti1-ruc/
mkfs.ext4 /dev/vdb
DISK_UUID=$(blkid -s UUID -o value /dev/vdb)
mkdir disks/$DISK_UUID
mount -t ext4 /dev/vdb /root/keti1-ruc/disks/$DISK_UUID
echo UUID=`sudo blkid -s UUID -o value /dev/path/to/disk` /mnt/disks/$DISK_UUID ext4 defaults 0 2 | sudo tee -a /etc/fstab

