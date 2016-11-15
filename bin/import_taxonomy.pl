#!/usr/bin/env perl

use strict;
use warnings;
use DBI;
use Getopt::Long;
use Data::Dumper;

=pod
=head1 Script import_taxonomy.pl
=head2 Description
This script creates a new taxonomy tree and puts it into the database
=head2 Authors
=over 
=item + Frank Foerster and Markus Ankenbrand
=back
=cut

# for logging purposes we use the Log4perl module
use Log::Log4perl;
use Log::Log4perl::Level;

my $conf = q(
    log4perl.category                  = INFO, Screen

    log4perl.appender.Screen         = Log::Log4perl::Appender::Screen
    log4perl.appender.Screen.stderr  = 1
    log4perl.appender.Screen.layout  = Log::Log4perl::Layout::SimpleLayout
);

Log::Log4perl->init(\$conf);
my $log = Log::Log4perl->get_logger();

my $VERSION = '0.1';

$log->debug("Started the generation of the taxonomy tree (".__FILE__.")");

my %options = (
	       "input"       => undef,
	       "help"        => undef,
	       "db_user"     => 'fennec',
           "db_password" => 'fennec',
           "db_name"     => 'fennec',
           "db_host"     => 'localhost',
           "db_port"     => 5432,
	       "transfer"    => 0,
	       "provider"    => undef,
           "description" => ''
	      );

$log->debug("Parsing the given commandline options");

# try to parse the options given via command line
if (! GetOptions( "help"          => \$options{help},
                  "input=s"       => \$options{input},
                  "db-user=s"     => \$options{db_user},
                  "db-password=s" => \$options{db_password},
                  "db-name=s"     => \$options{db_name},
                  "db-host=s"     => \$options{db_host},
                  "db-port=i"     => \$options{db_port},
                  "transfer!"     => \$options{transfer},
                  "provider=s"    => \$options{provider},
                  "description=s" => \$options{description}))
{
    # if the option parsing was not successful, we want to activate
    # the help function
    $options{help} = 1;
}

# if the help is wanted or provider/input parameter is missing
if ($options{help} || !$options{provider} || !$options{input})
{
    # TODO update help message
    # ...we will print the help message
    print "\n\n***** ".__FILE__." *** version: $VERSION *****\n\n";
    print "Allowed parameters:\n".
          "\t--[no]download  \tdo (not) fetch sequences from NCBI (default --download)\n".
	  "\t--[no]transfer  \tdo (not) transfer the nested set into the database (default --notransfer)\n".
	  "\t--[no]deletetemp\tdo (not) delete temporary files (default --deletetemp)\n".
	  "\t--help          \tthis message\n\n";

    # ...end exit without an error
    exit(0);
}

$log->info("***** ".__FILE__." *** version: $VERSION *****");

$log->info("Options: ".join(", ", map {"$_ = ".((defined $options{$_}) ? $options{$_} : "undef")} (keys %options)));

my $dbh = DBI->connect("dbi:Pg:dbname=$options{db_name};host=$options{db_host};port=$options{db_port};", $options{db_user}, $options{db_password});
unless($dbh){
    $log->logdie('Unable to connect to the database. Check parameters.');
}
$log->info('Successfully connected to the database.');

sub get_max_taxonomy_node_id{
    my $sth = $dbh->prepare('SELECT MAX(taxonomy_node_id) FROM taxonomy_node');
    $sth->execute();
    my $result = $sth->fetchall_arrayref();
    my $max_id = $result->[0][0];
    $max_id = 0 unless(defined($max_id));
    $log->info('The maximum taxonomy_node_id in the db prior to insertion is: '.$max_id);
    return $max_id;
}

sub get_max_right_idx{
    my $sth = $dbh->prepare('SELECT MAX(right_idx) FROM taxonomy_node');
    $sth->execute();
    my $result = $sth->fetchall_arrayref();
    my $max_idx = $result->[0][0];
    $max_idx = 0 unless(defined($max_idx));
    $log->info('The maximum right_idx in the db prior to insertion is: '.$max_idx);
    return $max_idx;
}

sub get_or_insert_provider{
    my $sth = $dbh->prepare('SELECT db_id FROM db WHERE name = ?');
    $sth->execute($options{provider});
    my $result = $sth->fetchall_arrayref();
    unless(@{$result}){
        $log->info("No provider with name $options{provider} found. Inserting...");
        $sth = $dbh->prepare('INSERT INTO db (name, description) VALUES (?, ?) RETURNING db_id');
        $sth->execute($options{provider}, $options{description});
        $result = $sth->fetchall_arrayref();
    }
    $log->info("Provider $options{provider} has id: $result->[0][0]");
    return $result->[0][0];
}

my $start_taxonomy_node_id = get_max_taxonomy_node_id() + 1;
my $start_left_idx = get_max_right_idx() + 1;
my $db_id = get_or_insert_provider();

# I need a function which I want to call recursivly
# here is the forward reference
sub getallchildren;

# and here the implementation

=pod
=head2 function getallchildren($$)
This function is necessary to generate the nested set.
It uses the information from the user input to get from the root to
all leaves and creates the nested set on the way to the leaves.
=head3 Parameters
=over
=item 1.
current fennec_id
=item 2.
current counter for left side of the leave
=item 3.
reference to the hash %parenttaxid2index
=item 4.
reference to the list @nodes
=item 5.
reference to the result list @nestedset
=item 6.
reference to the hash %fennec_id2node_id
=back
=cut

sub getallchildren
{
    my ($fennec_id, $lft, $parenttaxid2index, $nodes, $nestedset, $fennec_id2node_id) = @_;

    if (exists $parenttaxid2index->{$fennec_id})
    {
        # get the index range for the parent taxid from the mapping
        # hash %{$parenttaxid2index}
        my ($startindex, $endindex) = @{$parenttaxid2index->{$fennec_id}};
        # check if the endindex is undef... This can be possible, if
        # the parent taxid is used only once. In this case set the
        # endindex to the same value as the startindex
        if (! defined $endindex)
        {
            $endindex = $startindex;
        }
        $log->debug("The index range for the parent fennec_id $fennec_id is ".$startindex."-".$endindex);

        # go through the node list and call for each child the function recursively
        for (my $act_index = $startindex; $act_index <= $endindex; $act_index++)
        {
            # increase the lft
            $lft++;
            # get the rgt by recursivly calling getallchildren
            my $rgt = getallchildren($nodes->[$act_index]{fennec_id}, $lft, $parenttaxid2index, $nodes, $nestedset, $fennec_id2node_id);
            # push the value to the nested set
            push(@{$nestedset}, {
                     id => $nodes->[$act_index]{node_id},
                     fennec_id => $nodes->[$act_index]{fennec_id},
                     lft => $lft,
                     rgt => $rgt,
                     parent_id => $fennec_id2node_id->{$nodes->[$act_index]{parent_fennec_id}},
                     rank => $nodes->[$act_index]{rank}
                    });
            $lft=$rgt;
	    }

    } else
    {
	    $log->debug("Leave erreicht... fennec_id: $fennec_id; lft: $lft; rgt: ".($lft+1));
    }
    return $lft+1;
}

### TODO sub get_or_insert_provider
### TODO sub get_or_insert_rank

# Hash to map fennec_ids on node_ids
my %fennec_id2node_id = ();

# parse the whole file nodes.dmp and put it into the array @nodes
$log->info("Parsing the file '$options{input}'...");
my @nodes = ();
open(FH, "<$options{input}") || $log->logcroak("Unable to open the file $options{input} for reading!");
while (<FH>)
{
    chomp;
    my @dat = split(/\t/, $_);

    my $node = {fennec_id => int($dat[0]), parent_fennec_id => int($dat[1]), rank => int($dat[2]), node_id => int(@nodes)+$start_taxonomy_node_id};
    $fennec_id2node_id{$node->{fennec_id}} = $node->{node_id};
    push(@nodes, $node);
}
close(FH) || $log->logcroak("Unable to close the file $options{input} after reading!");
$log->info("Parsing the file '$options{input}' returned ".(scalar @nodes)." nodes.");

$log->info("Sorting the nodes by the parent-taxid and the taxid...");
@nodes = sort
{
    $a->{parent_fennec_id} <=> $b->{parent_fennec_id}
    ||
    $a->{fennec_id} <=> $b->{fennec_id}
} @nodes;
$log->info("Sorting the nodes by the parent-taxid and the taxid finished.");

# get the root node from the nodes list
my @root_nodes = grep {$_->{fennec_id} == $_->{parent_fennec_id}} @nodes;
if(@root_nodes != 1){
    $log->logdie('There should be exactly one root node (a node having itself as parent). Potential ids: '.(join(',', map {$_->{fennec_id}} @root_nodes)));
}
my $rootnode = $root_nodes[0];
@nodes = grep {$_->{fennec_id} != $rootnode->{fennec_id}} @nodes;

# the following list contains the nested set
my @nestedset = ();

# insert the rootnode into the nested set
push(@nestedset, {id => $rootnode->{node_id}, lft => $start_left_idx, rgt => $start_left_idx+1, parent_id => $fennec_id2node_id{$rootnode->{parent_fennec_id}}, rank => $rootnode->{rank}, fennec_id => $rootnode->{fennec_id}});

# generate a hash to map a parentid to a indexrange of the nodes list
$log->info("Creation of the parenttaxid2index hash...");
my %parenttaxid2index = ();
for (my $index = 0; $index < @nodes; $index++)
{
    if (exists $parenttaxid2index{$nodes[$index]{parent_fennec_id}})
    {
        # the current parenttaxid exists as key so I have to update the second item of the list
        $parenttaxid2index{$nodes[$index]{parent_fennec_id}}[1] = $index;
    } else
    {
        # the current parenttaxid does not exist so we have to create a new set
        $parenttaxid2index{$nodes[$index]{parent_fennec_id}} = [$index, undef];
    }
}
$log->info("Creation of the parenttaxid2index hash finished.");

# this is the loop to fill the nested set
$log->info("Insertion of all nodes into the nested set...");

# generate the nested set
$nestedset[0]{rgt}=getallchildren($nestedset[0]{fennec_id} ,$nestedset[0]{lft}, \%parenttaxid2index, \@nodes, \@nestedset, \%fennec_id2node_id);

$log->info("Insertion of all nodes into the nested set finished");

$log->info("Sorting the nested set by lft...");
@nestedset = sort 
{
    $a->{lft} <=> $b->{lft}
} @nestedset;
$log->info("Sorting the nested set by lft finished.");

if ($options{transfer})
{
    $log->info("Generating output for direct input into the database");

    my $dbcmd = "psql -h 172.18.0.2 -d fennectest -U fennectest -p 5432 -c \"\\copy taxonomy_node (taxonomy_node_id,parent_taxonomy_node_id,fennec_id,db_id,rank_id,left_idx,right_idx) FROM STDIN WITH NULL AS 'NULL' DELIMITER '\|'\"";

    open(DBOUT, "| ".$dbcmd) || $log->logdie("Unable to open the connection to the database: $!");
    foreach my $act_node (@nestedset)
    {
        #### TODO dbid from user input (provider, description -> create if not exists)
        my $str = join("|", $act_node->{id}, $act_node->{parent_id}, $act_node->{fennec_id}, $db_id, $act_node->{rank}, $act_node->{lft}, $act_node->{rgt});
        $log->debug("Insert the following line into the database: $str");
        print DBOUT "$str\n";
    }
    close(DBOUT) || $log->logdie("Unable to close the connection to the database $!");

    $log->info("Finished creation of the table!");
}


$log->info("Script ".__FILE__." finished.");


__END__