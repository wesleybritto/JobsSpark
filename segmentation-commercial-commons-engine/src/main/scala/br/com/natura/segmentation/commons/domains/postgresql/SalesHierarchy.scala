package br.com.natura.segmentation.commons.domains.postgresql

case class SalesHierarchy
(
person_id: String,
sales_hierarchy_cycle: Int
)

object SalesHierarchy {
  def empty: SalesHierarchy = {
    SalesHierarchy (
      person_id = "",
      sales_hierarchy_cycle = 0

    )
  }
}