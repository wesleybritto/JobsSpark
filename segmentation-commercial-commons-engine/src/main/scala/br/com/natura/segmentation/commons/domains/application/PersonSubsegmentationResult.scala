package br.com.natura.segmentation.commons.domains.application

import br.com.natura.segmentation.commons.domains.scylladb.PersonSubsegmentation

case class PersonSubsegmentationResult (
  consultant: ConsultantSegmentation,
  currentSubsegmentation: Option[PersonSubsegmentation],
  result: Boolean
)