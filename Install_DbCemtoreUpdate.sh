#!/bin/bash
> $PWD/Compile_Cemtore.log
> $PWD/Package_Version.log

echo "Create Domains / Types"
for type in `find $1 -path "*/Type/*.sql"`
do
  echo " compile script $type" |& tee -a $PWD/Compile_Cemtore.log
  psql -U cemtore -c 'SET client_min_messages = WARNING;' -f $type |& tee -a $PWD/Compile_Cemtore.log
done

echo "Create Tables"
for table in `find $1 -path "*/Table/*.sql"`
do
  echo " compile script $table" |& tee -a $PWD/Compile_Cemtore.log
  psql -U cemtore -c 'SET client_min_messages = WARNING;' -f $table |& tee -a $PWD/Compile_Cemtore.log
done

echo "Create Views"
for view in `find $1 -path "*/View/*.sql"`
do
  echo " compile script $view" |& tee -a $PWD/Compile_Cemtore.log
  psql -U cemtore -c 'SET client_min_messages = WARNING;' -f $view |& tee -a $PWD/Compile_Cemtore.log
done

echo "Create Triggers"
for trigger in `find $1 -path "*/Trigger/*.sql"`
do
  echo " compile script $trigger" |& tee -a $PWD/Compile_Cemtore.log
  psql -U cemtore -c 'SET client_min_messages = WARNING;' -f $trigger |& tee -a $PWD/Compile_Cemtore.log
done

echo "Create Functions"
for package in `find $1 -path "*/Pkg/*.sql"`
do
  echo " compile script $package" |& tee -a $PWD/Compile_Cemtore.log
  psql -U cemtore -f $package |& tee -a $PWD/Compile_Cemtore.log
done

echo "Update Configuration Data"
for updatescript in `find . -path "*/UpdateData/*.sql"`
do
  echo " compile script $updatescript" |& tee -a $PWD/Compile_Cemtore.log
  sed -i 's/\\\\/\\/g' $updatescript
  psql -U cemtore -f $updatescript |& tee -a $PWD/Compile_Cemtore.log
done

echo "Get Versions"
for version in `find . -path "*/Version/*.sql"`
do
  echo " compile script $version" |& tee -a $PWD/Package_Version.log
  sed -i 's/\\\\/\\/g' $version
  psql -U cemtore -f $version |& tee -a $PWD/Package_Version.log
done

Errors=`grep -c ERROR: $PWD/Compile_Cemtore.log`
if [ $Errors -eq 0 ]
then
  echo "No Compilation Error detected"
else
  echo "$Errors Compilation Errors detected"
  grep ERROR: $PWD/Compile_Cemtore.log |& tee -a $PWD/Compile_Cemtore.log
fi
