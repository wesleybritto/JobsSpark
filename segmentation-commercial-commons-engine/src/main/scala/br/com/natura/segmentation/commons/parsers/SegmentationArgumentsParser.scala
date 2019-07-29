package br.com.natura.segmentation.commons.parsers

import br.com.natura.segmentation.commons.domains.application.SegmentationArguments
import br.com.natura.segmentation.commons.enumerators.Arguments


object SegmentationArgumentsParser {
  def parse(implicit args: Array[String]): SegmentationArguments = {
    SegmentationArguments(
      country = this.getArgumentValue(Arguments.COUNTRY).get.toInt,
      multiple = this.getArgumentValue(Arguments.MULTIPLE).get.toBoolean,
      period =  this.parseString2Int(this.getArgumentValue(Arguments.PERIOD))
    )
  }

  def getArgumentValue(argument: String)(implicit args: Array[String]): Option[String] = {
    val argsFiltered = args.filter(_.contains(argument))

    if(argsFiltered.nonEmpty) {
      Some(argsFiltered(0).split("=")(1))
    } else {
      None
    }
  }

  def parseString2Int(arg: Option[String]): Int = {
    if(arg.nonEmpty) {
      arg.get.toInt
    } else {
      0
    }
  }
}