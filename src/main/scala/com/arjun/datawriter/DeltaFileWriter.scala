package com.arjun.datawriter

import com.arjun.datastructures.TargetTableDefinition
import org.apache.spark.sql.DataFrame

class DeltaFileWriter {


  def write(targetTableDefinition:TargetTableDefinition,inputDataFrame:DataFrame): Unit ={

    inputDataFrame.write
      .format("delta")
      .option("path", targetTableDefinition.table_location.replace("currated","staging"))
      .saveAsTable(s"${targetTableDefinition}_staging.${targetTableDefinition.table_name}")






  }




}
