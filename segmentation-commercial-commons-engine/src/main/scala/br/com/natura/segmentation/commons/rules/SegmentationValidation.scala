package br.com.natura.segmentation.commons.rules

import br.com.natura.segmentation.commons.domains.application.PersonSubsegmentationResult
import br.com.natura.segmentation.commons.enumerators.TableFields
import br.com.natura.segmentation.commons.handlers.spark.Spark
import br.com.natura.segmentation.commons.parsers.SegmentationDomain
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

class SegmentationValidation(rules: SegmentationRules) extends Serializable {
  val ss: SparkSession = Spark.getSession

  import ss.implicits._

  def validateRules(df: DataFrame): Dataset[PersonSubsegmentationResult] = {
    df.map(this.validateConsultantRule)
  }

  def validateConsultantRule(r: Row): PersonSubsegmentationResult = {
    val consultantRow = r.getStruct(r.fieldIndex(TableFields.CONSULTANT))
    val currentRow = r.getStruct(r.fieldIndex(TableFields.CURRENTSUBSEGMENTATION))
    val consultant = SegmentationDomain.parse2ConsultantSegmentation(consultantRow)
    val current = SegmentationDomain.parse2PersonSubsegmentation(currentRow)
    val method = rules.getClass.getMethod(consultant.subsegmentation_function, consultant.getClass, r.getClass)
    val result = method.invoke(rules, consultant, r).asInstanceOf[Boolean]

    PersonSubsegmentationResult(
      consultant = consultant,
      currentSubsegmentation = current,
      result = result
    )
  }
}
