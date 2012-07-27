#!/usr/bin/env perl

$| = 1;

use strict;

use Data::Dumper;
use Redis::hiredis;
use POSIX;
use Getopt::Long;
use Time::HiRes qw(gettimeofday);
use JSON;

my %children;
my $g_run_forrest_run = 1;
$SIG{TERM} = \&sigterm;
$SIG{CHLD} = \&REAPER;

my $json;
#open MSGS, "<../messages.json";
open MSGS, "<../sensor.json";
while (<MSGS>) { $json .= $_ }
close MSGS;
$json = decode_json($json);

my $children = 10;
my $limit = 0;
my $host = undef;
GetOptions(
    "children|c=i" => \$children,
    "limit|l=i" => \$limit,
    "host|s=s" => \$host
);

for ( my $i = 0; $i < $children; ++$i ) {
    &saturate();
}
pause() while(scalar(keys(%children)));

sub saturate {
    my $pid = fork();
    if($pid) {
        print "Child $pid started\n";
        $children{$pid}++;
        return $pid;
    }

    my $redis = Redis::hiredis->new();
    $redis->connect($host);

    my $cnt = 0;
    my $t0 = gettimeofday();
    while ($g_run_forrest_run ) {
        my $res = $redis->publish(1, $json->[int(rand(4))]);
        if ( $limit ) {
            ++$cnt;
            if ( $cnt == $limit ) {
                my $t1 = gettimeofday();
                print "reached my limit, sleeping (".($t1 - $t0).")\n";
                sleep(1);
                $cnt = 0;
                $t0 = gettimeofday();
            }
        }
    }
    exit 0;
}

sub sigterm {
    $g_run_forrest_run = 0;
}

sub REAPER {
    my $child;
    while ($child = waitpid(-1, WNOHANG)){
        last if $child == -1;
        print "Child $child exited\n";
        delete $children{$child};
    }
    $SIG{CHLD} = \&REAPER;
}
