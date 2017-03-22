package org.dianahep.sparkroot.defs

object ecal {
  /*
   * Definitions for CMS ECAL Subsystem in fasion similar to
   * https://github.com/cms-sw/cmssw/blob/CMSSW_8_1_X/DataFormats/EcalDigi/interface/EcalDataFrame.h
   * https://github.com/cms-sw/cmssw/blob/CMSSW_8_1_X/DataFormats/EcalDigi/interface/EcalMGPASample.h
   * https://github.com/cms-sw/cmssw/blob/CMSSW_8_1_X/DataFormats/Common/interface/DataFrameContainer.h
   */
  abstract class DataFrame(val id: Integer, val data: Seq[Short], val size: Integer);
  case class EcalSample(raw: Short) {
    val adc = raw & 0xFFF;
    val gainId = (raw >> 12) & 0x3;
  }
  case class EcalDataFrame(val id: Integer, val data: Seq[EcalSample],
                           val size: Integer);

  /*
   * Definitions for CMS ECAL Subsystem as to what actually sits on disk
   */
  case class DataFrameContainer(m_subdetId: Integer, m_stride: Integer,
                          m_ids: Seq[Integer], m_data: Seq[Short]);

  def convert2Readable(collection: DataFrameContainer): Seq[EcalDataFrame] = 
      for (i <- 0 until collection.m_ids.length) yield EcalDataFrame(
        collection.m_ids(i), 
        collection.m_data.slice(i*collection.m_stride, (i+1)*collection.m_stride)
          .map(EcalSample(_)),
        collection.m_stride)
}
