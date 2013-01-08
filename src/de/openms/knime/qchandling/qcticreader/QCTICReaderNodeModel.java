/**
 * Copyright (c) 2013, Stephan Aiche, Freie Universitaet Berlin
 * All rights reserved.
 * 
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *  * Redistributions of source code must retain the above copyright
 *    notice, this list of conditions and the following disclaimer.
 *  * Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in the
 *    documentation and/or other materials provided with the distribution.
 *  * Neither the name of the Freie Universitaet Berlin nor the
 *    names of its contributors may be used to endorse or promote products
 *    derived from this software without specific prior written permission.
 * 
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 * WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED. IN NO EVENT SHALL <COPYRIGHT HOLDER> BE LIABLE FOR ANY
 * DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
 * (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 * LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 * ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */
package de.openms.knime.qchandling.qcticreader;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.StringTokenizer;

import org.knime.core.data.DataCell;
import org.knime.core.data.DataColumnSpec;
import org.knime.core.data.DataColumnSpecCreator;
import org.knime.core.data.DataRow;
import org.knime.core.data.DataTableSpec;
import org.knime.core.data.RowKey;
import org.knime.core.data.def.DefaultRow;
import org.knime.core.data.def.DoubleCell;
import org.knime.core.data.uri.URIPortObject;
import org.knime.core.node.BufferedDataContainer;
import org.knime.core.node.BufferedDataTable;
import org.knime.core.node.CanceledExecutionException;
import org.knime.core.node.ExecutionContext;
import org.knime.core.node.ExecutionMonitor;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeLogger;
import org.knime.core.node.NodeModel;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.port.PortObject;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.node.port.PortType;

/**
 * This is the model implementation of QCTICReader.
 * 
 * @author Stephan Aiche
 */
public class QCTICReaderNodeModel extends NodeModel {

	private static final String SEPARATOR = "\t";
	// the logger instance
	private static final NodeLogger logger = NodeLogger
			.getLogger(QCTICReaderNodeModel.class);

	/**
	 * Static method that provides the incoming {@link PortType}s.
	 * 
	 * @return The incoming {@link PortType}s of this node.
	 */
	private static PortType[] getIncomingPorts() {
		return new PortType[] { URIPortObject.TYPE };
	}

	/**
	 * Static method that provides the outgoing {@link PortType}s.
	 * 
	 * @return The outgoing {@link PortType}s of this node.
	 */
	private static PortType[] getOutgoingPorts() {
		return new PortType[] { new PortType(BufferedDataTable.class) };
	}

	/**
	 * Constructor for the node model.
	 */
	protected QCTICReaderNodeModel() {
		super(getIncomingPorts(), getOutgoingPorts());
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	protected BufferedDataTable[] execute(final PortObject[] inData,
			final ExecutionContext exec) throws Exception {
		DataTableSpec outputSpec = createColumnSpec();
		BufferedDataContainer container = exec.createDataContainer(outputSpec);
		BufferedReader brReader = null;
		try {
			// read the data and fill the table
			File file2Read = new File(((URIPortObject) inData[0])
					.getURIContents().get(0).getURI());
			brReader = new BufferedReader(new InputStreamReader(
					new FileInputStream(file2Read)));

			int rowIdx = 1;

			// skip but check the header
			String header = brReader.readLine();
			StringTokenizer headerTokenizer = new StringTokenizer(header,
					SEPARATOR);
			if (headerTokenizer.countTokens() != 2)
				throw new Exception("Invalid file header");
			if (!"RT_(sec)".equals(headerTokenizer.nextToken()))
				throw new Exception("Invalid file header");
			if (!"TIC".equals(headerTokenizer.nextToken()))
				throw new Exception("Invalid file header");

			// for all lines
			String line;
			while ((line = brReader.readLine()) != null) {
				// skip empty line
				if ("".equals(line.trim()))
					continue;

				StringTokenizer st = new StringTokenizer(line, SEPARATOR);
				DataCell[] cells = new DataCell[2];

				if (st.countTokens() != 2)
					throw new Exception(
							"Invalid qcml-TIC file. Offending line: nr="
									+ rowIdx + "; " + line);

				cells[0] = new DoubleCell(Double.parseDouble(st.nextToken()));
				cells[1] = new DoubleCell(Double.parseDouble(st.nextToken()));

				RowKey key = new RowKey("Row " + rowIdx);
				DataRow row = new DefaultRow(key, cells);
				container.addRowToTable(row);

				exec.checkCanceled();
				++rowIdx;
			}

		} catch (Exception ex) {
			logger.error(ex.getMessage());
			throw ex;
		} finally {
			if (brReader != null)
				brReader.close();
		}
		container.close();
		BufferedDataTable out = container.getTable();
		return new BufferedDataTable[] { out };
	}

	private DataTableSpec createColumnSpec() {
		DataColumnSpec[] allColSpecs = new DataColumnSpec[2];
		allColSpecs[0] = new DataColumnSpecCreator("RT", DoubleCell.TYPE)
				.createSpec();
		allColSpecs[1] = new DataColumnSpecCreator("TIC", DoubleCell.TYPE)
				.createSpec();
		DataTableSpec outputSpec = new DataTableSpec(allColSpecs);
		return outputSpec;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	protected void reset() {
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	protected DataTableSpec[] configure(final PortObjectSpec[] inSpecs)
			throws InvalidSettingsException {
		return new DataTableSpec[] { createColumnSpec() };
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	protected void saveSettingsTo(final NodeSettingsWO settings) {
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	protected void loadValidatedSettingsFrom(final NodeSettingsRO settings)
			throws InvalidSettingsException {
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	protected void validateSettings(final NodeSettingsRO settings)
			throws InvalidSettingsException {
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	protected void loadInternals(final File internDir,
			final ExecutionMonitor exec) throws IOException,
			CanceledExecutionException {
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	protected void saveInternals(final File internDir,
			final ExecutionMonitor exec) throws IOException,
			CanceledExecutionException {
	}

}
