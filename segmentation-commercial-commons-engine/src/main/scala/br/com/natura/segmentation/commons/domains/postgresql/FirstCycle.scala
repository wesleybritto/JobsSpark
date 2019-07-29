package br.com.natura.segmentation.commons.domains.postgresql

case class FirstCycle
(
  person_code: Int,
  first_cycle: Int
)


object FirstCycle {
  def empty: FirstCycle = {
    FirstCycle (
      person_code = 0,
      first_cycle = 0

    )
  }
}