# sandy_read
sandy_read
use File::Find;
use File::Copy;
use Cwd;


$basedir = getcwd;

$dir = $basedir."/Sample_FileInputs_Directory";  # this is the directory where the csv files are stored
find(\&process_files, $dir);
&process_files();

# --------------------------- subroutines used by this script ------------------------------
sub process_files()
# a subroutine to go through all the files in the current dir and process only csv files
{
     if ( -f and /.csv?/ )
     {
          my $file_to_open = $_;
          my $full_path_name =$File::Find::name;
          print "File name is $file_to_open\n\t\tFull path is $File::Find::name\n";
          print "Reading file $file_to_open\n\t\t\n";
          if (substr($file_to_open,0,13) eq "Daily_Summary")
          {
              &translate_daily_summary_file($file_to_open,$full_path_name);
          }
          if (substr($file_to_open,0,10) eq "Daily_vsan")
          {
               &translate_daily_pd_space_file($file_to_open,$full_path_name);
          }
          if (substr($file_to_open,0,13) eq "Daily_AO_CPG_")
          {
               &translate_cpg_region_file($file_to_open,$full_path_name);
          }
          if (substr($file_to_open,0,9) eq "Daily_VV_")
          {
               &translate_high_res_file($file_to_open,$full_path_name);
          }
          if (substr($file_to_open,0,8) eq "Daily_Ho")
          {
               &translate_high_res_file($file_to_open,$full_path_name);
          }
          if (substr($file_to_open,0,10) eq "Daily_CPGD")
          {
               &translate_daily_cpgd_info($file_to_open,$full_path_name);
          }
          if (substr($file_to_open,0,10) eq "Daily_CPGR")
          {
               &translate_daily_cpgr_info($file_to_open,$full_path_name);
          }

          if (substr($file_to_open,0,9) eq "Daily_VVR")
          {
               &translate_daily_vvr($file_to_open,$full_path_name);
          }
          if (substr($file_to_open,0,10) eq "Daily_VLUN")
          {
               &translate_daily_vlun($file_to_open,$full_path_name);
          }
     }
}

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
sub translate_daily_summary_file()
{
        $file=$_[0];    # file to open
        $full_path =$_[1];
        print "Processing ",$full_path,"\n";
        open(INFO,$file);           # open the file for input
        @lines=<INFO>;              # read the file into an array
        close(INFO);                # close the file
        $i=0;
        $total_records = 0;        # count number of data records in the file
        foreach $row (@lines)
        {
                chop $row;
                @columns=split(/,/,$row);  # split the line into separate columns using the commas
                if ($i == 0)
                {
                     $time_stamp = substr($row,18,19);
                }
                if ($i == 2)
                {
                     for ($j = 0; $j < (scalar @columns); $j++)
                     {
                       $field_text = $columns[$j];
                       $field_text =~s/ /_/g;          # replace space with underscore
                       $field_text =~s/\///g;          # remove /
                       $field_text =~s/7K/_VII_K/g;
                       $field_text =~s/7.2K/_VII_K/g;
                       $field_text =~s/10K/_X_K/g;
                       $field_text =~s/15K/_XV_K/g;
                       $field_text =~s/RAID0/RAID_ZERO/g;
                       $field_text =~s/RAID1/RAID_ONE/g;
                       $field_text =~s/RAID5/RAID_FIVE/g;
                       $field_text =~s/RAID6/RAID_SIX/g;
                       $field_text =~s/%/_PCT/g;       # replace % symbol
                       $field_text =~s/__/_/g;         # replace double underscore
                       $data[$total_records][$j] = uc($field_text)."_C";
                       #$data[$total_records][$j] = $field_text;
                     }
                     $total_columns = (scalar @columns);
                     $total_records++;
                }
                if ($i > 2)
                {
                     for ($j = 0; $j < (scalar @columns); $j++)
                     {
                       $data[$total_records][$j] = $columns[$j];
                     }
                     $total_records++;
                }
       $i=$i+1;
        }


       #~~~~~~~~~~~~~~~~ write output file ~~~~~~~~~~~~~~~~

       for ($j = 1; $j < ($total_records-1); $j++)
       {
                   $outfile= substr($full_path,0,-4)."_".$data[$j][0].".txt";
                open(OUTPUT,">$outfile"); #open file for output
            print OUTPUT "OBJNM,VALUE,INTVL,TS\n";

              for ($i = 1; $i < $total_columns; $i++)
              {
                    print OUTPUT $data[0][$i],",",$data[$j][$i],",86400,",$time_stamp,"\n";
              }
        }

        close (OUTPUT);      #close the current file and open a new one to write to

}
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
sub translate_daily_pd_space_file()
{
        $file=$_[0];    # file to open
        $full_path =$_[1];
        print "Processing ",$full_path,"\n";
        open(INFO,$file);           # open the file for input
        @lines=<INFO>;              # read the file into an array
        close(INFO);                # close the file
        $i=0;
        $total_records = 0;        # count number of data records in the file
        foreach $row (@lines)
        {
                chop $row;
                if ($i == 0)
                {
                     $time_stamp = substr($row,30,19);
                }
                if ($i == 2)
                {    $row_mod = $row;
                     $row_mod =~s/, /_/g;   #replace ", " with "_"
                     $row_mod =~s/"//g;   #remove quotes
                       $row_mod =~s/ /_/g;          # replace space with underscore
                       $row_mod =~s/\///g;          # remove /
                       $row_mod =~s/7K/_VII_K/g;
                       $row_mod =~s/7.2K/_VII_K/g;
                       $row_mod =~s/10K/_X_K/g;
                       $row_mod =~s/15K/_XV_K/g;
                       $row_mod =~s/RAID0/RAID_ZERO/g;
                       $row_mod =~s/RAID1/RAID_ONE/g;
                       $row_mod =~s/RAID5/RAID_FIVE/g;
                       $row_mod =~s/RAID6/RAID_SIX/g;
                       $row_mod =~s/%/_PCT/g;       # replace % symbol
                       $row_mod =~s/__/_/g;         # replace double underscore
                       #$data[$total_records][$j] = uc($row_mod)."_C";

                     @columns=split(/,/,$row_mod);  # split the line into separate columns using the commas
                     for ($j = 0; $j < (scalar @columns); $j++)
                     {
                       $data[$total_records][$j] = $columns[$j];
                     }
                     $total_columns = (scalar @columns);
                     $total_records++;
                }
                if ($i > 2)
                {
                     @columns=split(/,/,$row);  # split the line into separate columns using the commas
                     for ($j = 0; $j < (scalar @columns); $j++)
                     {
                       $data[$total_records][$j] = $columns[$j];
                     }
                     $total_records++;
                }
        $i=$i+1;
        }


       #~~~~~~~~~~~~~~~~ write output file ~~~~~~~~~~~~~~~~

       for ($j = 1; $j < ($total_records-1); $j++)
       {
                   $outfile= substr($full_path,0,-4)."_".$data[$j][0].".txt";
                open(OUTPUT,">$outfile"); #open file for output
            print OUTPUT "OBJNM,VALUE,INTVL,TS\n";

              for ($i = 1; $i < $total_columns; $i++)
              {
                    print OUTPUT uc($data[0][$i]),"_C,",$data[$j][$i],",86400,",$time_stamp,"\n";
              }
        }

        close (OUTPUT);      #close the current file and open a new one to write to

}
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
sub translate_cpg_region_file()
{
        $file=$_[0];    # file to open
        $full_path =$_[1];
        print "Processing ",$full_path,"\n";
        open(INFO,$file);           # open the file for input
        @lines=<INFO>;              # read the file into an array
        close(INFO);                # close the file
        $i=0;
        $total_records = 0;        # count number of data records in the file
        foreach $row (@lines)
        {
                chop $row;
                if ($i == 0)
                {
                     $time_stamp = substr($row,51,19);
                }
                if ($i == 2)
                {    $row_mod = $row;
                     $row_mod =~s/ /_/g;   #replace " " with "_"
                     @columns=split(/,/,$row_mod);  # split the line into separate columns using the commas
                     for ($j = 0; $j < (scalar @columns); $j++)
                     {
                       $data[$total_records][$j] = $columns[$j];
                     }
                     $total_columns = (scalar @columns);
                     $total_records++;
                }
                if ($i > 2)
                {
                     @columns=split(/,/,$row);  # split the line into separate columns using the commas
                     for ($j = 0; $j < (scalar @columns); $j++)
                     {
                       $data[$total_records][$j] = $columns[$j];
                     }
                     $total_records++;
                }
        $i=$i+1;
        }


       #~~~~~~~~~~~~~~~~ write output file ~~~~~~~~~~~~~~~~


       for ($j = 1; $j < ($total_records-1); $j++)
       {
                       $outfile= substr($full_path,0,-4)."_".$data[$j][0].".txt";
                open(OUTPUT,">$outfile"); #open file for output
                print OUTPUT "SUBOBJNM,VALUE,INTVL,OBJNM,TS\n";

              for ($i = 0; $i < $total_columns; $i++)
              {
                    print OUTPUT $data[0][$i],",",$data[$j][$i],",86400,HP_METRIC_COUNT_C,",$time_stamp,"\n";
              }
        }

        close (OUTPUT);      #close the current file and open a new one to write to

}
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
sub translate_high_res_file()
{
        $file=$_[0];    # file to open
        $full_path =$_[1];
        print "Processing ",$full_path,"\n";
        open(INFO,$file);           # open the file for input
        @lines=<INFO>;              # read the file into an array
        close(INFO);                # close the file
        $i=0;
        $total_records = 0;        # count number of data records in the file
        foreach $row (@lines)
        {
                chop $row;
                if ($i == 2)
                {    $row_mod = $row;
                     $row_mod =~s/ /_/g;   #replace " " with "_"
                       $row_mod =~s/\///g;          # remove /
                       $row_mod =~s/7K/_VII_K/g;
                       $row_mod =~s/7.2K/_VII_K/g;
                       $row_mod =~s/10K/_X_K/g;
                       $row_mod =~s/15K/_XV_K/g;
                       $row_mod =~s/RAID0/RAID_ZERO/g;
                       $row_mod =~s/RAID1/RAID_ONE/g;
                       $row_mod =~s/RAID5/RAID_FIVE/g;
                       $row_mod =~s/RAID6/RAID_SIX/g;
                       $row_mod =~s/%/_PCT/g;       # replace % symbol
                       $row_mod =~s/__/_/g;         # replace double underscore
                       #$data[$total_records][$j] = uc($row_mod)."_C";
                     @columns=split(/,/,$row_mod);  # split the line into separate columns using the commas
                     for ($j = 0; $j < (scalar @columns); $j++)
                     {
                       $data[$total_records][$j] = $columns[$j];
                     }
                     $total_columns = (scalar @columns);
                     $total_records++;
                }
                if ($i > 2)
                {
                     @columns=split(/,/,$row);  # split the line into separate columns using the commas
                     for ($j = 0; $j < (scalar @columns); $j++)
                     {
                       $data[$total_records][$j] = $columns[$j];
                     }
                     $total_records++;
                }
        $i=$i+1;
        }


       #~~~~~~~~~~~~~~~~ write output file ~~~~~~~~~~~~~~~~

              $outfile= substr($full_path,0,-4)."_".$data[$j][0].".txt";
        open(OUTPUT,">$outfile"); #open file for output
       print OUTPUT "OBJNM,VALUE,INTVL,TS\n";

       if ($full_path=~"Port")
       {
            for ($j = 1; $j < ($total_records-1); $j++)
            {
                print OUTPUT uc($data[0][1]),"_C,",$data[$j][1],",3600,",&time_stamp($data[$j][0]),"\n";
                print OUTPUT uc($data[0][2]),"_C,",$data[$j][2],",3600,",&time_stamp($data[$j][0]),"\n";
                print OUTPUT uc($data[0][4]),"_C,",$data[$j][4],",3600,",&time_stamp($data[$j][0]),"\n";
                print OUTPUT uc($data[0][5]),"_C,",$data[$j][5],",3600,",&time_stamp($data[$j][0]),"\n";
                print OUTPUT uc($data[0][9]),"_C,",$data[$j][9],",3600,",&time_stamp($data[$j][0]),"\n";
                print OUTPUT uc($data[0][13]),"_C,",$data[$j][13],",3600,",&time_stamp($data[$j][0]),"\n";
             }
        }
       if ($full_path=~"VV")
       {
            for ($j = 1; $j < ($total_records-1); $j++)
            {
                print OUTPUT uc($data[0][1]),"_C,",$data[$j][1],",3600,",&time_stamp($data[$j][0]),"\n";
                print OUTPUT uc($data[0][2]),"_C,",$data[$j][2],",3600,",&time_stamp($data[$j][0]),"\n";
                print OUTPUT uc($data[0][4]),"_C,",$data[$j][4],",3600,",&time_stamp($data[$j][0]),"\n";
                print OUTPUT uc($data[0][5]),"_C,",$data[$j][5],",3600,",&time_stamp($data[$j][0]),"\n";
                print OUTPUT uc($data[0][6]),"_C,",$data[$j][6],",3600,",&time_stamp($data[$j][0]),"\n";
                print OUTPUT uc($data[0][8]),"_C,",$data[$j][8],",3600,",&time_stamp($data[$j][0]),"\n";
             }
        }

        close (OUTPUT);      #close the current file and open a new one to write to

}
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
sub time_stamp
{  # a subroutine to generate a date and time stamp for the High_Res files
   # A parameter are received $_[0] which is of the format "d hh:mm"
   # This needs to be converted into yyyy-mm-dd hh:mm:ss

#  my ($d,$t);
#  my ($sec,$min,$hour,$mday,$mon,$year,$wday,$yday,$isdst) = localtime(time);
   use Time::Local;
   my $today = timelocal 0, 0, 12, ( localtime )[3..5];
   my ($d, $mon, $year) = ( localtime $today-86400 )[3..5];

        @cols=split(/ /,$_[0]);
        $year += 1900;
        $mon++;
        $mon = sprintf("%2d",$mon);
        $year = sprintf("%4d",$year);
        $mon=~ tr/ /0/;
        $t = $cols[1].":00";
        $time_stmp = $year."-".$mon."-".$cols[0]." ".$t;
        return($time_stmp);
}

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

sub time_stamp_2
{  # a subroutine to generate a date and time stamp for the High_Res files
   # A parameter are received $_[0] which is of the format "d hh:mm"
   # This needs to be converted into yyyy-mm-dd hh:mm:ss

   my ($d,$t);
   my ($sec,$min,$hour,$mday,$mon,$year,$wday,$yday,$isdst) = localtime(time);
   use Time::Local;

        @cols=split(/ /,$_[0]);
        $year += 1900;
        $mon++;
        $mon = sprintf("%2d",$mon);
        $year = sprintf("%4d",$year);
        $mon=~ tr/ /0/;
        $time_stmp = $year."-".$mon."-".$mday." ".$hour.":".$min.":".$sec;
        return($time_stmp);
}

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~



sub translate_daily_cpgd_info()
{
        $file=$_[0];    # file to open
        $full_path =$_[1];
        print "Processing ",$full_path,"\n";
        open(INFO,$file);           # open the file for input
        @lines=<INFO>;              # read the file into an array
        close(INFO);                # close the file
        $total_records = 0;        # count number of data records in the file
        $time_stamp = &time_stamp_2(localtime(time));
        foreach $row (@lines)
        {
                chop $row;
                @columns=split(/,/,$row);  # split the line into separate columns using the commas
                if($columns[1] eq 'total')
                {
                        $user_total = $columns[7];
                        $user_used = $columns[8];
                        $snap_total = $columns[9];
                        $snap_used = $columns[10];
                        $admin_total = $columns[11];
                        $admin_used = $columns[12];
                }
        }


       #~~~~~~~~~~~~~~~~ write output file ~~~~~~~~~~~~~~~~
            $outfile= substr($full_path,0,-4).".txt";
            open(OUTPUT,">$outfile"); #open file for output
            print OUTPUT "OBJNM,VALUE,INTVL,TS\n";
            print OUTPUT "CPG_TOTAL_C,",($user_total + $snap_total + $admin_total),",86400,",$time_stamp,"\n";
            print OUTPUT "CPG_USED_C,",($user_used + $snap_used + $admin_used),",86400,",$time_stamp,"\n";

        close (OUTPUT);      #close the current file and open a new one to write to

}

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

sub translate_daily_cpgr_info()
{
        $file=$_[0];    # file to open
        $full_path =$_[1];
        print "Processing ",$full_path,"\n";
        open(INFO,$file);           # open the file for input
        @lines=<INFO>;              # read the file into an array
        close(INFO);                # close the file
        $total_records = 0;        # count number of data records in the file
        $time_stamp = &time_stamp_2(localtime(time));
        foreach $row (@lines)
        {
                chop $row;
                @columns=split(/,/,$row);  # split the line into separate columns using the commas
                if($columns[1] eq 'total')
                {
                        $user_total = $columns[7];
                        $user_rtotal = $columns[8];
                        $user_used = $columns[9];
                        $user_rused = $columns[10];
                        $snap_total = $columns[11];
                        $snap_rtotal = $columns[12];
                        $snap_used = $columns[13];
                        $snap_rused = $columns[14];
                        $admin_total = $columns[15];
                        $admin_rtotal = $columns[16];
                        $admin_used = $columns[17];
                        $admin_rused = $columns[18];
                }
        }


       #~~~~~~~~~~~~~~~~ write output file ~~~~~~~~~~~~~~~~
            $outfile= substr($full_path,0,-4).".txt";
            open(OUTPUT,">$outfile"); #open file for output
            print OUTPUT "OBJNM,VALUE,INTVL,TS\n";
            print OUTPUT "CPG_TOTAL_C,",($user_total + $snap_total + $admin_total),",86400,",$time_stamp,"\n";
            print OUTPUT "CPG_RTOTAL_C,",($user_rtotal + $snap_rtotal + $admin_rtotal),",86400,",$time_stamp,"\n";
            print OUTPUT "CPG_USED_C,",($user_used + $snap_used + $admin_used),",86400,",$time_stamp,"\n";
            print OUTPUT "CPG_RUSED_C,",($user_rused + $snap_rused + $admin_rused),",86400,",$time_stamp,"\n";

        close (OUTPUT);      #close the current file and open a new one to write to

}

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

sub translate_daily_vvr()
{
       $file=$_[0];    # file to open
        $full_path =$_[1];
        print "Processing ",$full_path,"\n";
        open(INFO,$file);           # open the file for input
        @lines=<INFO>;              # read the file into an array
        close(INFO);                # close the file
        $i=0;
        $time_stamp = &time_stamp_2(localtime(time));
                $j=0;
                $k=0;
                $total_records = 0;        # count number of data records in the file
        foreach $row (@lines)
        {
                chop $row;
                @columns=split(/,/,$row);  # split the line into separate columns using the commas
                for ($j = 0; $j < (scalar @columns); $j++)
                     {
                       $data[$total_records][$j] = $columns[$j];
                     }
                $total_columns = (scalar @columns);
                $total_records++;

        }
        $i=$i+1;


       #~~~~~~~~~~~~~~~~ write output file ~~~~~~~~~~~~~~~~

       $outfile= substr($full_path,0,-4).".txt";
       open(OUTPUT,">$outfile"); #open file for output
       print OUTPUT "OBJNM,SUBOBJNM,VALUE,INTVL,TS\n";

            for ($j = 1; $j < ($total_records); $j++)
            {
               print OUTPUT "ADM_RAWRSVD_C,",uc($data[$j][1])," : ",$data[$j][2],",",$data[$j][4],",86400,",$time_stamp,"\n";
               print OUTPUT "ADM_RSVD_C,",uc($data[$j][1])," : ",$data[$j][2],",",$data[$j][5],",86400,",$time_stamp,"\n";
               print OUTPUT "SNP_RAWRSVD_C,",uc($data[$j][1])," : ",$data[$j][2],",",$data[$j][6],",86400,",$time_stamp,"\n";
               print OUTPUT "SNP_RSVD_C,",uc($data[$j][1])," : ",$data[$j][2],",",$data[$j][7],",86400,",$time_stamp,"\n";
               print OUTPUT "USR_RAWRSVD_C,",uc($data[$j][1])," : ",$data[$j][2],",",$data[$j][8],",86400,",$time_stamp,"\n";
               print OUTPUT "USR_RSVD_C,",uc($data[$j][1])," : ",$data[$j][2],",",$data[$j][9],",86400,",$time_stamp,"\n";
               print OUTPUT "TOT_RAWRSVD_C,",uc($data[$j][1])," : ",$data[$j][2],",",$data[$j][10],",86400,",$time_stamp,"\n";
               print OUTPUT "TOT_RSVD_C,",uc($data[$j][1])," : ",$data[$j][2],",",$data[$j][11],",86400,",$time_stamp,"\n";
               print OUTPUT "TOT_VSIZE_C,",uc($data[$j][1])," : ",$data[$j][2],",",$data[$j][12],",86400,",$time_stamp,"\n";

            }
        close (OUTPUT);      #close the current file and open a new one to write to

}
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
sub translate_daily_vlun()
{
}
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

