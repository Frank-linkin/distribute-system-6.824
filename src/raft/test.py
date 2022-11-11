import sys
import getopt


just = False
opts,args=getopt.getopt(sys.argv[1:],"c:l:",[])
print(opts,args)
for opt_name,opt_value in opts:
    if opt_name == '-l' :
        names=opt_value.split(" ")
        just = True
    if opt_name == '-c':
        n_columns = 5

just = '1233'
if just
    print('just SOSO')
