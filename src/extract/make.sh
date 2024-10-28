#!/bin/sh -x
pgm=extract



export DELLIBS=`dellib phdst`
export CERNLIBS=`cernlib  genlib packlib kernlib`


ycmd=`which nypatchy`
command="$ycmd - $pgm.f $pgm.cra $pgm.ylog .go"
echo "Running $command"
eval $command


# compile
shopt -s nullglob
for ending in .f .F ; do
    for file in *$ending  ; do
        $FCOMP $FFLAGS -c $file
    done
done

$FCOMP $LDFLAGS *.o -o $pgm.exe $DELLIBS $CERNLIBS

