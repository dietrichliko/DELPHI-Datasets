#!/bin/bash -x


# run.sh /eos/experiment/delphi/castor2015/MCprod/cern/hzha03pythia6156/v94c/91.2/hzha03pythia6156_tttt_91.2_4_4_41007.sdst

here="$(dirname "$(readlink -f "$0")")"

mkdir  /tmp/extract_$$.tmp
cd /tmp/extract_$$.tmp
echo "FILE = $1" > PDLINPUT

$here/extract.exe

