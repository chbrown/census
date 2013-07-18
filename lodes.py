import os
import urllib
import subprocess

states = [
    ('al', 'Alabama'),
    ('ak', 'Alaska'),
    ('az', 'Arizona'),
    ('ar', 'Arkansas'),
    ('ca', 'California'),
    ('co', 'Colorado'),
    ('ct', 'Connecticut'),
    ('de', 'Delaware'),
    ('dc', 'District of Columbia'),
    ('fl', 'Florida'),
    ('ga', 'Georgia'),
    ('hi', 'Hawaii'),
    ('id', 'Idaho'),
    ('il', 'Illinois'),
    ('in', 'Indiana'),
    ('ia', 'Iowa'),
    ('ks', 'Kansas'),
    ('ky', 'Kentucky'),
    ('la', 'Louisiana'),
    ('me', 'Maine'),
    ('md', 'Maryland'),
    ('ma', 'Massachusetts'),
    ('mi', 'Michigan'),
    ('mn', 'Minnesota'),
    ('ms', 'Mississippi'),
    ('mo', 'Missouri'),
    ('mt', 'Montana'),
    ('ne', 'Nebraska'),
    ('nv', 'Nevada'),
    ('nh', 'New Hampshire'),
    ('nj', 'New Jersey'),
    ('nm', 'New Mexico'),
    ('ny', 'New York'),
    ('nc', 'North Carolina'),
    ('nd', 'North Dakota'),
    ('oh', 'Ohio'),
    ('ok', 'Oklahoma'),
    ('or', 'Oregon'),
    ('pa', 'Pennsylvania'),
    ('pr', 'Puerto Rico'),
    ('ri', 'Rhode Island'),
    ('sc', 'South Carolina'),
    ('sd', 'South Dakota'),
    ('tn', 'Tennessee'),
    ('tx', 'Texas'),
    ('ut', 'Utah'),
    ('vt', 'Vermont'),
    ('va', 'Virginia'),
    ('wa', 'Washington'),
    ('wv', 'West Virginia'),
    ('wi', 'Wisconsin'),
    ('wy', 'Wyoming')]


def test_gzip(filepath):
    # returns True if and only if 1) there is a file at filepath and
    # 2) `gzip -t :filepath` has an exit code of 0
    return os.path.exists(filepath) and subprocess.call(['gzip', '-t', filepath]) == 0


def download(url, dirpath, verbose=True):
    filepath = os.path.join(dirpath, os.path.basename(url))
    # if test_gzip(filepath):
    # print 'Deleting corrupt gzip:', filepath
    # os.remove(filepath)
    if os.path.exists(filepath):
        print 'Local file already exists:', filepath
        return filepath
    else:
        tmpfile, headers = urllib.urlretrieve(url)
        if verbose:
            print 'Downloaded: ' + url
            print '>', tmpfile
            print '!', headers
        os.rename(tmpfile, filepath)
        if verbose:
            print 'mv %s %s' % (tmpfile, filepath)
        return filepath


def loop(dirpath, versions):
    root = 'http://lehd.did.census.gov/onthemap/{Version}/{State}'

    if not os.path.exists(dirpath):
        os.mkdir(dirpath)

    for version in versions:
        for state, state_name in states:
            args = dict(Version=version, State=state)
            print 'Starting {StateName} with {Version}'.format(StateName=state_name, Version=version)

            download((root + '/version.txt').format(**args), dirpath)
            download((root + '/{State}_xwalk.csv.gz').format(**args), dirpath)
            md5sum_filename = download((root + '/lodes_{State}.md5sum').format(**args), dirpath)

            with open(md5sum_filename) as md5sum_fp:
                for line in md5sum_fp:
                    md5sum, filename = line.split()
                    if 'xwalk' not in filename:
                        _, Type, _ = filename.split('_', 2)
                        url = (root + '/{Type}/{filename}.gz').format(Type=Type, filename=filename, **args)
                        download(url, dirpath)


def main(dirpath, versions, nattempts=100):
    # Retry until we run without raising an exception
    for i in range(nattempts):
        print 'Attempt #%d' % i
        try:
            loop(dirpath, versions)
        except Exception, exc:
            print exc
        else:
            break
    else:
        print 'Tried the maximum amount of times (%d)' % nattempts


if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser(description='Download all LODES census data')
    parser.add_argument('dirpath', help='directory to hold all files, without hierarchy')
    parser.add_argument('--versions', nargs='+', default=['LODES7'])
    opts = parser.parse_args()

    main(opts.dirpath, opts.versions)
