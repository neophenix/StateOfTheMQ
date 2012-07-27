#!/usr/bin/env perl

$| = 1;

use lib './lib';
use strict;

use Data::Dumper;
use Kafka::Producer;
use POSIX;
use Getopt::Long;
use Time::HiRes qw(gettimeofday);
use JSON;

my %children;
my $g_run_forrest_run = 1;
$SIG{TERM} = \&sigterm;
$SIG{CHLD} = \&REAPER;

my $children = 1;
my $limit = 0;
my $host = undef;
GetOptions(
    "children|c=i" => \$children,
    "limit|l=i" => \$limit,
    "host|h=s" => \$host
);

my $json;
#open IN,"<../messages.json";
open IN,"<../sensor.json";
while (<IN>) { $json .= $_ }
close IN;
my $messages = decode_json($json);

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

    my $cnt = 0;
    my $p = Kafka::Producer->new(host => $host, port => 9092);
    my $t0 = gettimeofday();
    while ($g_run_forrest_run ) {
        $p->send(topic => 'test', messages => [ $messages->[int(rand(4))] ]);
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
