#!/bin/bash

date=$(date '+%Y%m%d')
run=$((10000000 + RANDOM % 99999999))
file_n=${date}_M1_${run}.005_D_1ES1959_650-W0.40_000.root
file_size=10M
RSE_origin=PIC-DCACHE
RSE_destiny=INFN-NA-DPM
RSE_QOS=QOS=FAST
scope_n=MAGIC_PIC_BRUZZESE

echo "this file will be generated $file_n"

dd if=/dev/urandom of=$file_n bs=$file_size count=1

echo "$file_n will be replicated under the scope $scope_n to the RSE $RSE_n"

rule_parent=$(rucio -vvv upload --scope $scope_n --rse $RSE_origin $file_n --summary)
#rucio update-rule --lifetime 10 $rule_parent
rucio delete-rule --purge-replicas --all --rse_expression $RSE_origin --account bruzzese $scope_n:$file_n

echo "$run dataset $run will be created"

rucio add-dataset $scope_n:$run

rucio attach $scope_n:$run $scope_n:$file_n
  
rule_child_1=$(rucio add-rule $scope_n:$run 1 $RSE_destiny)

# As an alternative, you could also change the lifetime of a particular rule like this :
## rucio update-rule --lifetime 10 $rule_child
rucio delete-rule --purge-replicas --all --rse_expression $RSE_destiny --account bruzzese $rule_child_1 


# As an alternative, you could also change the lifetime of a particular rule like this :
## rucio update-rule --lifetime 10 $rule_child
rule_child_2=$(rucio add-rule $scope_n:$run 1 $RSE_QOS)

#rucio update-rule --lifetime 10 $rule_child
rucio delete-rule --purge-replicas --all --rse_expression $RSE_QOS --account bruzzese $rule_child_2 

echo "$file_n will be listed the scope $scope_n "

rucio list-rules $scope_n:$file_n

echo "$file_n will be discovered under the paths : "

rucio list-dids $scope_n:$file_n --filter type="ALL"

echo "$file_n will be located under the paths : "

rucio list-file-replicas $scope_n:$file_n

