# ETLReportProcessor
<b>DataWarehousing and ETL based Data Processor for Reporting in Visual Studio .NET/C#</b>
<ul>

<li> Performs ETL based Datawarehousing functionality of reading files (CSV, VCF etc) from a specific folder and processes them</li>
<li> Extract, parses, filters, sanitizes and normalizes based on rules</li>

<li> Handles various file formats
    <ul>
      <li>CSVs</li>
      <li>XML</li>
      <li>RDFs(Semantic Format)</li>
      <li>JSON</li>
      <li>VCF (Variant Call Format)</li>
      <li>BAM</li>
      <li>BLAST/FASTA(Genome Sequencing Format)</li>
      <li>ARFF(Attribute Relation File Format)</li>
    </ul>
<li> Intelligently performs a fuzzy match to detect Titles </li>
<li> Performs semantic mapping when files with similar semantical schema is loaded based on the rules on each field.
<li> Loads the clean data after sanitization into database that can be configured and allows for reporting from the database. </li>
<li> Current repository version showcases VCF (Variant Call Format) File formats for loading them and moving the processed files into separate folders.</li>
<li> Allows logging messages during the execution.</li>
<li> Logging provided at the record and field level </li>
</ul>


The application once built, can be run either from the IDE or from the command-line as an executable and all the related configuration for the data processor can be set in the Config file.
Allows for the application to be tied with a monitoring and scheduling software so as to run it periodically to extract, normalize, purge and load into Excel files or databases.
