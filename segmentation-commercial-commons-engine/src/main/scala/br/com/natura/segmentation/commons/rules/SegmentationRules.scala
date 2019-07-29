package br.com.natura.segmentation.commons.rules

import br.com.natura.segmentation.commons.domains.application.ConsultantSegmentation
import br.com.natura.segmentation.commons.enumerators.TableFields
import br.com.natura.segmentation.commons.parsers.SegmentationDomain
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema

abstract class SegmentationRules extends Serializable {
  def validatePerformanceIndex(consultant: ConsultantSegmentation, r: GenericRowWithSchema): Boolean = {
    val indexRow = r.getStruct(r.fieldIndex(TableFields.CONSULTANTINDEX))
    val index = SegmentationDomain.parse2ConsultantIndex(indexRow)

    consultant.index_value.get == index.index_value
  }
}
