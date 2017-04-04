#!/usr/bin/perl -w
# Creates an html table of flight delays by weather for the given route

# Needed includes
use strict;
use warnings;
use 5.10.0;
use HBase::JSONRest;
use CGI qw/:standard/;

# Read the origin and destination airports as CGI parameters
my $batter = param('batter');
my $pitcher = param('pitcher');
 
# Define a connection template to access the HBase REST server
# If you are on out cluster, hadoop-m will resolve to our Hadoop master
# node, which is running the HBase REST server. The first version
# is for our VM, the second is for running on the class cluster
# my $hbase = HBase::JSONRest->new(host => "localhost:8080");
my $hbase = HBase::JSONRest->new(host => "hdp-m.c.mpcs53013-2016.internal:2056");

# This function takes a row and gives you the value of the given column
# E.g., cellValue($row, 'delay:rain_delay') gives the value of the
# rain_delay column in the delay family.
# It uses somewhat tricky perl, so you can treat it as a black box
sub cellValue {
    my $row = $_[0];
    my $field_name = $_[1];
    my $row_cells = ${$row}{'columns'};
    foreach my $cell (@$row_cells) {
	if ($$cell{'name'} eq $field_name) {
	    return $$cell{'value'};
	}
    }
    return 0;
}

# Query hbase for the route. For example, if the departure airport is ORD
# and the arrival airport is DEN, the "where" clause of the query will
# require the key to equal ORDDEN
my $records2015 = $hbase->get({
  table => 'jakecoll_batter_v_pitcher_2015',
  where => {
    key_equals => $batter.$pitcher
  },
});

my $records2014 = $hbase->get({
  table => 'jakecoll_batter_v_pitcher_2014',
  where => {
    key_equals => $batter.$pitcher
  },
});

my $records2013 = $hbase->get({
  table => 'jakecoll_batter_v_pitcher_2013',
  where => {
    key_equals => $batter.$pitcher
  },
});


# There will only be one record for this route, which will be the
# "zeroth" row returned
my $row15 = @$records2015[0];
my $row14 = @$records2014[0];
my $row13 = @$records2013[0];


# Get the value of all the columns we need and store them in named variables
# Perl's ability to assign a list of values all at once is very convenient here
my($pa15, $ab15, $k15, $walk15, $iwalk15, $hbp15, $h1b15, $h2b15, $h3b15, $hr15,
	$pa14, $ab14, $k14, $walk14, $iwalk14, $hbp14, $h1b14, $h2b14, $h3b14, $hr14,
	$pa13, $ab13, $k13, $walk13, $iwalk13, $hbp13, $h1b13, $h2b13, $h3b13, $hr13)
 =  (cellValue($row15, 'stats:PA'), cellValue($row15, 'stats:AB'),
     cellValue($row15, 'stats:K'), cellValue($row15, 'stats:walk'),
     cellValue($row15, 'stats:IW'), cellValue($row15, 'stats:HBP'),
     cellValue($row15, 'stats:h1B'), cellValue($row15, 'stats:h2B'),
     cellValue($row15, 'stats:h3B'), cellValue($row15, 'stats:HR'),
     cellValue($row14, 'stats:PA'), cellValue($row14, 'stats:AB'),
     cellValue($row14, 'stats:K'), cellValue($row14, 'stats:walk'),
     cellValue($row14, 'stats:IW'), cellValue($row14, 'stats:HBP'),
     cellValue($row14, 'stats:h1B'), cellValue($row14, 'stats:h2B'),
     cellValue($row14, 'stats:h3B'), cellValue($row14, 'stats:HR'),
     cellValue($row13, 'stats:PA'), cellValue($row13, 'stats:AB'),
     cellValue($row13, 'stats:K'), cellValue($row13, 'stats:walk'),
     cellValue($row13, 'stats:IW'), cellValue($row13, 'stats:HBP'),
     cellValue($row13, 'stats:h1B'), cellValue($row13, 'stats:h2B'),
     cellValue($row13, 'stats:h3B'), cellValue($row13, 'stats:HR')
);

# Given the number of flights and the total delay, this gives the average delay
sub average_d {
    my($hit, $appearance) = @_;
    return $appearance > 0 ? ($hit/$appearance) : "-";
}

# Print an HTML page with the table. Perl CGI has commands for all the
# common HTML tags
print header, start_html(-title=>'hello CGI',-head=>Link({-rel=>'stylesheet',-href=>'/table.css',-type=>'text/css'}));

print div({-style=>'margin-left:275px;margin-right:auto;display:inline-block;box-shadow: 10px 10px 5px #888888;border:1px solid #000000;-moz-border-radius-bottomleft:9px;-webkit-border-bottom-left-radius:9px;border-bottom-left-radius:9px;-moz-border-radius-bottomright:9px;-webkit-border-bottom-right-radius:9px;border-bottom-right-radius:9px;-moz-border-radius-topright:9px;-webkit-border-top-right-radius:9px;border-top-right-radius:9px;-moz-border-radius-topleft:9px;-webkit-border-top-left-radius:9px;border-top-left-radius:9px;background:white'}, '&nbsp;Batter Stats For ' . $batter . ' vs ' . $pitcher . ' by Year&nbsp;');
print     p({-style=>"bottom-margin:10px"});
print table({-class=>'CSS_Table_Example', -style=>'width:60%;margin:auto;'},
	    Tr([td(['Year', 'PA', 'AB', 'K', 'BB', 'HBP', '1B', '2B', '3B', 'HR','AVG']),
		td(['2013', $pa13, $ab13, $k13, $walk13 + $iwalk13, $hbp13, $h1b13, $h2b13, $h3b13, $hr13, average_d($h1b13+$h2b13+$h3b13+$hr13,$ab13)]),
		td(['2014', $pa14, $ab14, $k14, $walk14 + $iwalk14, $hbp14, $h1b14, $h2b14, $h3b14, $hr14, average_d($h1b14+$h2b14+$h3b14+$hr14,$ab14)]),
                td(['2015', $pa15, $ab15, $k15, $walk15 + $iwalk15, $hbp15, $h1b15, $h2b15, $h3b15, $hr15, average_d($h1b15+$h2b15+$h3b15+$hr15,$ab15)]),
		td(['Total', $pa15+$pa14+$pa13, $ab15+$ab14+$ab13, $k15+$k14+$k13, $walk15 + $iwalk15 + $walk14 + $iwalk14 + $walk13 + $iwalk13, $hbp15 + $hbp14 + $hbp13, $h1b15 + $h1b14 + $h1b13, $h2b15 + $h2b14 + $h2b13, $h3b15 + $h3b14 + $h3b13, $hr15 + $hr14 + $hr13, average_d($h1b15+$h2b15+$h3b15+$hr15+$h1b14+$h2b14+$h3b14+$hr14+$h1b13+$h2b13+$h3b13+$hr13,$ab15 + $ab14 + $ab13)])
		])),
    p({-style=>"bottom-margin:10px"})
    ;

print end_html;
