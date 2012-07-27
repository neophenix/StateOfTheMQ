#!/usr/bin/env perl

use strict;

$| = 1;

use Data::Dumper;
use Net::RabbitMQ;
use Getopt::Long;
use Time::HiRes qw(gettimeofday);
use POSIX;
use JSON;

my $children = 10;
my $etype = 'topic';
my $rkey = '#';
my $host = undef;
GetOptions(
    "children|c=i" => \$children,
    "exchange|e=s" => \$etype,
    "routing|r=s" => \$rkey,
    "host|h=s" => \$host
);

my $DEBUG = 1;

my %children;
my $g_run_forrest_run = 1;
$SIG{TERM} = \&sigterm;
$SIG{CHLD} = \&REAPER;
$SIG{INT} = \&sigterm;

for ( my $i = 0; $i < $children; ++$i ) {
    &run($i);
}
pause() while(scalar(keys(%children)));

exit 0;

sub run {
    my $id = shift;
    my $pid = fork();
    if($pid) {
        print "Child $pid started\n";
        $children{$pid}++;
        return $pid;
    }

    my ($mq, $q);
    my ($low, $high, $run, $total_s, $total_m, $avg) = (50000, 0, 0, 0, 0, 0);

    print "CONNECTING...\n" if $DEBUG;
    $mq = Net::RabbitMQ->new();
    $mq->connect($host, { user => 'guest', password => 'guest' });
    $mq->channel_open(1);
    $mq->exchange_declare(1, 'saturate', { exchange_type => $etype, durable => 0, auto_delete => 1 });
    $q = $mq->queue_declare(1, 'getting.saturated.'.$$, {exclusive => 1, auto_delete => 1});
    $mq->queue_bind(1, $q, 'saturate', '#');
    $mq->consume(1, $q);

    print "RUNNING...\n" if $DEBUG;
    my $i = 0;
    my $t0 = gettimeofday();
    my $t1 = gettimeofday();
    my $per_sec = 0;
    eval {
        while ( my $p = $mq->recv() ) {
            $i++;
            $t1 = gettimeofday();
            if ( $t1 - $t0 >= 1 ) {
                $per_sec = ($i / ($t1 - $t0));
                $high = $per_sec if ( $run > 1 && $per_sec > $high );            
                $low = $per_sec if ( $run > 1 && $per_sec < $low );
                $total_s += ($t1 - $t0) if ( $run > 1 );
                $total_m += $i if ( $run > 1 );
                $avg = ($total_m / $total_s) if ( $run > 1 );
                $run++;
                print "MSG/s: $per_sec\t\tAvg: $avg\n";

                $t0 = gettimeofday();
                $i = 0;
            }
            last if ( ! $g_run_forrest_run );
        }
    };
    &stop($low, $high, $avg);
}

sub stop {
    my ($low, $high, $avg) = @_;
    print "Totals: [$low] [$high] [$avg]\n";
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
