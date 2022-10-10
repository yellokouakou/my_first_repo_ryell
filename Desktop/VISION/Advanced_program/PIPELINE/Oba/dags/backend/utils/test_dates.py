from dates_generator import *


def main(filename):
    with open(filename) as fichier:
        lines = fichier.readlines()

    for line in lines:
        strttest = line #"#old--> start: 2021-07-18 end: 2021-10-03"
        if strttest.rstrip() != "":
            print(strttest)
            sidx = strttest.find("start:")
            eidx = strttest.find("end:")
            start_str = strttest[sidx+7:sidx+17]
            end_str = strttest[eidx+5:eidx+15]

            print("start: "+ start_str)
            print("end: "+ end_str)

            dates = generate_dates_from_weeks(3,"20211118")
            #dates_new = generate_weeks(start_str, end_str)
            dates_new = generate_months(start_str, end_str)

def sec_main():
    start_str, end_str = generate_previous_week()
    print("start: "+ start_str)
    print("end: "+ end_str)    


if __name__=="__main__":
#    main("data/oba_file.txt")
    sec_main()
