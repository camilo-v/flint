# Tax ID to Lineage
   ### taxIDtoLineage.py
   The script for extending input file with a taxonomic tree. The input file should contain the column with taxID. Output file structure:

   | costum columns from input file | tax_id | phylum | class | order | family | species |

   You can specify taxonomic ranks in *--ranks* argument. For example, with parameter *--ranks phylum,class* the output file will have the structure:

   | costum columns from input file | taxID | phylum | class |

   #### Command line Options:

      Required arguments:
          --input                 (str)       a file containing a column with NCBI taxID
          --output                (str)       the output file containing the lineage columns
                                              ('superkingdom', 'phylum', 'class', 'order', 'family', 'genus', 'species')
                                              and other custom columns (see parameter --include_columns)

          --tax_col               (int)       the position of column with taxID (counting starts from 0).

      Optional arguments:
          --include_columns       (str)       the columns from input file to be included to output file
                                              sequence of int with comma delimiter
                                              "1,2,4" - include columns 1, 2, and 4 from input file to the result file.

          --include_columns_names (str)       Column names of included columns.
                                              By default the program uses column names from the input file.
                                              Names should be comma separated.
                                              For example, if include_columns="1,4"
                                              --include columns_names "assemblyID,strain" will rename
                                              the default column names from the input.

          --email                 (str)       Set the Entrez email parameter (default is not set)

          --ranks                 (str)       Optional. Specifies which ranks to use with coma sepparator.
                                              By default: --ranks superkindom, phylum, class, order, family, species
             Dependancies:
                  Biopython

#### Example of usage
   Below is the example of how to get a taxonomic tree from EnsemblBacteria metadata file using taxIDtoLineage.py

   Metadata could be downloaded from http://bacteria.ensembl.org/info/website/ftp/index.html. Here is the example of downloading annotations for release 38 using unix <i>wget</i> command:

      wget ftp://ftp.ensemblgenomes.org/pub/release-38/bacteria/species_EnsemblBacteria.txt

  There is a part of file species_EnsemblBacteria.txt:

  <img width="1211" alt="screen shot 2018-05-20 at 7 22 34 pm" src="https://user-images.githubusercontent.com/29647319/40284938-dcf0b14a-5c63-11e8-94c6-22a8259cd048.png">
  Let's say we want to get taxonomic tree for each row, and we want to include columns "#name" and "assembly" to our output file. There is bash script to do this:

      #!/bin/bash
      # initializing constances:
      # --------------------------------------------------------------------------
      VERSION=38
      INPUT='species_EnsemblBacteria.txt'
      OUTPUT='tax_tree_'$VERSION'.txt'
      TAX_COL=3 #column with taxID
      INCLUDE_COLUMNS='4,1' #colum numbers to be included to output
      INCLUDE_COLUMNS_NAMES="assemblyID,strain"
      EMAIL='vsteb002@fiu.edu'
      # --------------------------------------------------------------------------
      python taxIDtoLineage.py --input $INPUT \
						--output $OUTPUT \
						--tax_col $TAX_COL \
						--include_columns $INCLUDE_COLUMNS \
						--include_columns_names $INCLUDE_COLUMNS_NAMES \
						--email $EMAIL


 Output file will include the folowing colomns:

assemblyID    | strain |	taxonomy_id |	superkingdom |	phylum |	class |	order |	family |	genus |	species
