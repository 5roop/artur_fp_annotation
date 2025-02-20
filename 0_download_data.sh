curl --remote-name-all https://www.clarin.si/repository/xmlui/bitstream/handle/11356/1863/Gos.TEI.zip
unzip Gos.TEI.zip
rm -f Gos.TEI.zip


curl --remote-name-all https://www.clarin.si/repository/xmlui/bitstream/handle/11356/1776{/Artur-J-Audio_00.tar,/Artur-J-Audio_01.tar,/Artur-J-Audio_02.tar,/Artur-J-Audio_03.tar,/Artur-J-Audio_04.tar,/Artur-J-Audio_05.tar,/Artur-J-Audio_06.tar,/Artur-N-Audio_00.tar,/Artur-N-Audio_01.tar,/Artur-N-Audio_02.tar,/Artur-N-Audio_03.tar,/Artur-P-Audio_00.tar,/Artur-P-Audio_01.tar,/Artur-P-Audio_02.tar,/Artur-P-Audio_03.tar,/Artur-P-Audio_04.tar,/Artur-P-Audio_05.tar}

for file in $(ls *.tar)
do
    tar -xvf $file
done

rm -f *.tar