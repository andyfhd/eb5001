- Vagrant shared dir (install vagrant first)
- Move the whole folder to desired location
- install cygwin terminal to execute commands
- cmd: vagrant up (auto import to Virtual Box and bring up VM)
- cmd: vagrant ssh (ssh into VM)
- flume.conf will automatically be in shared path with VM
- install flume (mine is 1.9.0 but doesn't really matter)
- copy jar to this shared dir to submit


Useful Commands
vagrant

start flume agent (/vagrant/apache-flume-1.9.0-bin)
bin/flume-ng agent --conf ./conf/ -f conf/flume.conf -Dflume.root.logger=DEBUG,console -n agent_review


submit local
/pluralsight/spark/bin/spark-submit --class streaming.StreamingJob2 /vagrant/yelp-1.0-SNAPSHOT-shaded.jar


submit cluster
/pluralsight/spark/bin/spark-submit --class streaming.StreamingJob --master yarn --deploy-mode cluster /vagrant/yelp-1.0-SNAPSHOT-shaded.jar




host
vagrant/source python review_source_api.py
