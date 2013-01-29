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
package de.openms.knime.qchandling;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;

import org.knime.core.data.DataCell;
import org.knime.core.data.DataRow;
import org.knime.core.data.RowKey;
import org.knime.core.data.def.DefaultRow;
import org.knime.core.node.BufferedDataContainer;
import org.knime.core.node.ExecutionContext;
import org.knime.core.node.NodeLogger;

/**
 * Helper class to ease the process of reading in the QC TSV files.
 * 
 * @author aiche
 */
public abstract class TSVReader {

	/**
	 * Construct a TSVReader for the given number of columns.
	 * 
	 * @param numberOfColumns
	 *            The number of expected columns.
	 * @param ignoreAdditionalContent
	 *            If true additional columns that do not fit to the expected
	 *            format are silently ignored instead of generating an error.
	 *            Default is false.
	 */
	public TSVReader(final int numberOfColumns,
			final boolean ignoreAdditionalContent) {
		m_numberOfColumns = numberOfColumns;
		m_ignoreAdditionalContent = ignoreAdditionalContent;
	}

	public TSVReader(final int numberOfColumns) {
		this(numberOfColumns, false);
	}

	/**
	 * The number of columns of the tsv file to read.
	 */
	private final int m_numberOfColumns;

	/**
	 * Flag indicating if additional columns are ignored or reported as error.
	 */
	private final boolean m_ignoreAdditionalContent;

	/**
	 * The logger instance.
	 */
	private static final NodeLogger logger = NodeLogger
			.getLogger(TSVReader.class);

	/**
	 * The TSV separator.
	 */
	private static final String SEPARATOR = "\t";

	/**
	 * The header of the tsv file to parse.
	 * 
	 * @return A String array containing all the column headers.
	 */
	protected abstract String[] getHeader();

	/**
	 * Checks if the header elements found in the file correspond to those
	 * defined by the deriving class.
	 * 
	 * @param header
	 *            The header that should be tested.
	 * @throws Exception
	 *             If the headers do not match.
	 */
	private void compareHeader(String[] header) throws Exception {
		for (int i = 0; i < m_numberOfColumns; ++i) {
			if (!header[i].equals(getHeader()[i])) {
				throw new Exception("Invalid header element: Expected "
						+ getHeader()[i] + " but got " + header[i] + ".");
			}
		}
	}

	/**
	 * The parse method extracting the different values for the current line.
	 * 
	 * @param tokens
	 *            An array of Strings containing the values that should be
	 *            extracted.
	 * @return
	 */
	protected abstract DataCell[] parseLine(String[] tokens) throws Exception;

	public void run(File tsvFile, BufferedDataContainer container,
			final ExecutionContext exec) throws Exception {
		BufferedReader brReader = null;
		try {
			// read the data and fill the table
			brReader = new BufferedReader(new InputStreamReader(
					new FileInputStream(tsvFile)));

			int rowIdx = 1;

			// skip but check the header
			String header = brReader.readLine();
			String[] headerElements = header.trim().split(SEPARATOR, -1);

			if (((headerElements.length > m_numberOfColumns) && !m_ignoreAdditionalContent)
					|| (headerElements.length < m_numberOfColumns))
				throw new Exception("Invalid file header. Expected "
						+ m_numberOfColumns + " columns but got "
						+ headerElements.length + ".");

			compareHeader(headerElements);

			// for all lines
			String line;
			while ((line = brReader.readLine()) != null) {
				// skip empty line
				if ("".equals(line.trim()))
					continue;

				String[] tokens = line.trim().split(SEPARATOR, -1);

				try {
					// we try to parse and leave it to the deriving class to
					// check if everything is correct
					DataCell[] cells = parseLine(tokens);

					RowKey key = new RowKey("Row " + rowIdx);
					DataRow row = new DefaultRow(key, cells);
					container.addRowToTable(row);
				} catch (Exception ex) {
					throw new Exception(
							"Invalid qcml-TIC file. Offending line: nr="
									+ rowIdx + "; " + line);
				}

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
	}
}
