#!/usr/bin/perl

$| = 1;

use lib './lib';
use strict;
use Kafka::Consumer;
use Kafka::FetchRequest;
use Kafka::OffsetRequest;
use Data::Dumper;
use Getopt::Long;
use Time::HiRes qw(gettimeofday);

my $host = undef;
GetOptions(
    "host|h=s" => \$host
);

my $c = Kafka::Consumer->new(host => $host, port => 9092);
my $f = Kafka::FetchRequest->new(topic => 'test', partition => 0, offset => 0);
my $o = Kafka::OffsetRequest->new(topic => 'test', partition => 0);

my $lng_offset = 0;
my $offset = 0;

my $msg_cnt = 0;
my ($low, $high, $run, $total_s, $total_m, $avg) = (50000, 0, 0, 0, 0, 0);
my $t0 = gettimeofday();
my $t1 = gettimeofday();
my $per_sec = 0;
while (1) {
    eval {
        my $m = $c->fetch($f);
        if ( $m && ! $m->{'error'} ) {
            $t1 = gettimeofday();
            if ( $t1 - $t0 >= 1 ) {
                $per_sec = ($msg_cnt / ($t1 - $t0));
                $high = $per_sec if ( $run > 1 && $per_sec > $high );
                $low = $per_sec if ( $run > 1 && $per_sec < $low );
                $total_s += ($t1 - $t0) if ( $run > 1 );
                $total_m += $msg_cnt if ( $run > 1 );
                $avg = ($total_m / $total_s) if ( $run > 1 );
                $run++;
                print "MSG/s: $per_sec\t\tAvg: $avg\n";

                $t0 = gettimeofday();
                $msg_cnt = 0;
            }

            foreach my $msg ( @{$m->get_messages()} ) {
                $msg_cnt++;
            #    print $msg->message() . "\n";
            }
            #print $m->get_messages()->[0]->message() . "\n";
            $lng_offset = $offset;
            $offset += $m->total_bytes();
            $f->offset($offset);
        }
    };
    if ( $@ ) {
        #warn $@;
        $offset = $lng_offset;
    }
}
