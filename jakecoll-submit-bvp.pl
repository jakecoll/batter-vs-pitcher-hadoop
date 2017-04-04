#!/usr/bin/perl -w
# Program: cass_sample.pl
# Note: includes bug fixes for Net::Async::CassandraCQL 0.11 version

use strict;
use warnings;
use 5.10.0;
use FindBin;

use Scalar::Util qw(
        blessed
    );
use Try::Tiny;

use Kafka::Connection;
use Kafka::Producer;

use Data::Dumper;
use CGI qw/:standard/, 'Vars';

my $batter = param('batter');
my $pitcher = param('pitcher');
if(!$batter) {
    exit;
}
if(!$pitcher) {
    exit;
}

my $matchup = $batter.$pitcher;

my $pa = param('PA') ? 1 : 0;
my $ab = param('AB') ? 1 : 0;
my $k = param('K') ? 1 : 0;
my $bb = param('walk') ? 1 : 0;
my $iwalk = param('IW') ? 1 : 0;
my $hbp = param('HBP') ? 1 : 0;
my $h1b = param('h1B') ? 1 : 0;
my $h2b = param('h2B') ? 1 : 0;
my $h3b = param('h3B') ? 1 : 0;
my $hr = param('HR') ? 1 : 0;

my ( $connection, $producer );
try {
    #-- Connection
    # $connection = Kafka::Connection->new( host => 'localhost', port => 6667 );
    $connection = Kafka::Connection->new( host => 'hdp-m.c.mpcs53013-2016.internal', port => 6667 );

    #-- Producer
    $producer = Kafka::Producer->new( Connection => $connection );
    # Only put in the station_id and weather elements because those are the only ones we care about
    my $message = "<current_observation><matchup>".$matchup."</matchup><stats>";
    if($pa) { $message .= "PA "; }
    if($ab) { $message .= "AB "; }
    if($k) { $message .= "K "; }
    if($bb) { $message .= "walk "; }
    if($iwalk) { $message .= "IW "; }
    if($hbp) { $message .= "HBP "; }
    if($h1b) { $message .= "h1B "; }
    if($h2b) { $message .= "h2B "; }
    if($h3b) { $message .= "h3B "; }
    if($hr) { $message .= "HR "; }
    $message .= "</stats></current_observation>";

    # Sending a single message
    my $response = $producer->send(
	'jakecoll-bvp-events',          # topic
	0,                                 # partition
	$message                           # message
        );
} catch {
    if ( blessed( $_ ) && $_->isa( 'Kafka::Exception' ) ) {
	warn 'Error: (', $_->code, ') ',  $_->message, "\n";
	exit;
    } else {
	die $_;
    }
};

# Closes the producer and cleans up
undef $producer;
undef $connection;

print header, start_html(-title=>'Submit BvP Results',-head=>Link({-rel=>'stylesheet',-href=>'/table.css',-type=>'text/css'}));
print table({-class=>'CSS_Table_Example', -style=>'width:80%;'},
            caption('Batter vs Pitcher Event Submitted'),
	    Tr([th(["matchup","PA","AB","K","BB","iBB","HBP","1B","2B","3B","HR"]),
	        td([$matchup, $pa, $ab, $k, $bb, $iwalk, $hbp, $h1b, $h2b, $h3b, $hr])]));

#print $protocol->getTransport->getBuffer;
print end_html;

