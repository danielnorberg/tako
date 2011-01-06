import pstats
import argparse

def main():
    parser = argparse.ArgumentParser(description="Profile analyzer")
    parser.add_argument('file', help='profile file')
    args = parser.parse_args()
    p = pstats.Stats(args.file)
    p.strip_dirs().sort_stats('time').print_stats(50)
    p.strip_dirs().sort_stats('cumulative').print_stats(50)

if __name__ == '__main__':
    main()
