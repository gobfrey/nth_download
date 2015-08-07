#!/usr/bin/perl -I/opt/eprints3/perl_lib

use strict;
use warnings;

use EPrints;

my ($repoid, $n) = @ARGV;

die 'nth_download *repository_id* *n*' unless $n;

chomp $n;
die "non-integer value: $n\n" unless $n =~ m/^\d+$/;

my $ep = EPrints->new();
my $repo = $ep->repository( $repoid );

die "Couldn't create repository object for $repoid\n" unless $repo;

my $db = $repo->database;

my $sql = "SHOW TABLES LIKE 'irstats2_downloads'";
if (query($db, $sql))
{
        print render_result(nth_download_irstats($db, $n), $n) . "\n";
        print "This result was generated from the IRStats2 downloads table\n\n";
}

print render_result(nth_download_no_irstats($db, $n), $n) . "\n";
print "This result was generated from the EPrints access table\n\n";

sub render_result
{
        my ($result) = @_;

        return "ERROR: " . $result->{ERR}  if $result->{ERR};
        return join(' ', 'EPrint', $result->{eprint}, "was download number $n on", $result->{datestamp});
}

sub nth_download_irstats
{
        my ($db, $n) = @_;

        #verify we have enough downloads
        my $sql = <<END;
SELECT
        SUM(count)
FROM
        irstats2_downloads
END
        my $total_downloads = query($db, $sql);
        return { 'ERR' => 'Only ' . $total_downloads->[0] } unless $total_downloads->[0] >= $n;

        #iterative search -- should do better
        $sql = <<END;
SELECT
        eprintid, count, datestamp
FROM
        irstats2_downloads
ORDER BY
        uid
END

        my $sth = $db->prepare($sql);
        $sth->execute;

        my $total_count = 0;
        my $eprintid;
        my $datestamp;
        while (my $r = $sth->fetchrow_arrayref)
        {
                $total_count += $r->[1];
                if ($total_count >= $n)
                {
                        $eprintid = $r->[0];
                        $datestamp = $r->[2];
                        last;
                }
        }
        return { eprint => $eprintid, datestamp => $datestamp };
}

sub nth_download_no_irstats
{
        my ($db, $n) = @_;

        my $sql = <<END;
SELECT
        COUNT(*)
FROM
        access
WHERE
        service_type_id = '?fulltext=yes'
END
        my $total_downloads = query($db, $sql);
        return { 'ERR' => 'Only ' . $total_downloads->[0] } unless $total_downloads->[0] >= $n;

        $sql = <<END;
SELECT
        referent_id, datestamp_year, datestamp_month, datestamp_day
FROM
        access
WHERE
        service_type_id = '?fulltext=yes'
LIMIT
        $n,1
END
        my $row = query($db, $sql);
        return {
                eprint => $row->[0],
                datestamp => sprintf("%04d", $row->[1]) . sprintf("%02d", $row->[2]) . sprintf("%02d", $row->[3])
        }
}

sub query
{
        my ($db, $sql) = @_;

        my $sth = $db->prepare($sql);
        $sth->execute;

        if ($sth->rows)
        {
                return $sth->fetchrow_arrayref;
        }
        return [];
  }
