import sys

from cpc_class_tables import parse_and_write_cpc_class

if __name__ == '__main__':

    if len(sys.argv) != 3:
        print("Usage: python {} {} {}".format(sys.argv[0],
                                              "<INPUT DIR>", "<OUTPUT DIR>"))
        sys.exit(1)

    inputdir = sys.argv[1]
    outputdir = sys.argv[2]

    print("Parsing and writing CPC Classification tables... \n")
    print("INPUT DIRECTORY: {}".format(inputdir))
    print("OUTPUT DIRECTORY: {}".format(outputdir))

    parse_and_write_cpc_class(inputdir, outputdir)
