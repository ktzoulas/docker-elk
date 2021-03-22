import json
import optparse
import time
import subprocess


def fetch(filepath):
    """Fetch content of the given file into memory."""
    with open(filepath, 'r') as infile:
        return json.load(infile)


def netcat(data, host, port):
    """Run netcat command for the given batch of JSON data. Returns `False` if an error occurs."""
    process_1 = subprocess.Popen(["echo", json.dumps(data)], stdout=subprocess.PIPE)
    process_2 = subprocess.Popen(["nc", "-c", host, port], stdin=process_1.stdout, stdout=subprocess.PIPE)

    (_, stderr) = process_2.communicate()
    time.sleep(1)

    if stderr:
        print(f" - failed to send the last dataset to logstash: {stderr}")
        return False


if __name__ == '__main__':
    parser = optparse.OptionParser("Usage: %prog [options] jsonfile")
    parser.add_option('-l', '--limit', dest='limit', default=None, type="int", help="restrict the number of features")
    parser.add_option('--host', dest='host', default='localhost', help='define the host')
    parser.add_option('--port', dest='port', default='5000', help="define the port")
    
    (options, args) = parser.parse_args()
    if not args:
        parser.error("incorrect number of arguments!")

    print(f" - fetch the {args[0]} file into memory.")
    data = fetch(args[0])

    counter = 0
    print(" - feed logstash feature-by-feature:")

    for feature in data:
        if options.limit and options.limit == counter: break

        counter += 1
        print(f" :: [feature#{counter}]")
        
        netcat(feature, options.host, options.port)
