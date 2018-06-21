import sys

from cpc_class_tables import parse_and_write_cpc

if __name__ == '__main__':

    if len(sys.argv) != 3:
        print("Usage: python {} {} {}".format(sys.argv[0],
                                              "<INPUT DIR>", "<OUTPUT DIR>"))
        sys.exit(1)

    inputdir = sys.argv[1]
    outputdir = sys.argv[2]

    print("Parsing and writing CPC Classification tables... \n")
    print("INPUT DIRECTORY: {} \n".format(inputdir))
    print("OUTPUT DIRECTORY: {} \n".format(outputdir))

    parse_and_write_cpc(inputdir, outputdir)
