/*
 *  Copyright (C) 2018 MET Norway
 *  Contact information:
 *  Norwegian Meteorological Institute
 *  Henrik Mohns Plass 1
 *  0313 OSLO
 *  NORWAY
 *
 * This file is part of metsis-metadata
 * metsis-metadata is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 *  the Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version.
 * metsis-metadata is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU General Public License for more details.
 * You should have received a copy of the GNU General Public License
 * along with metsis-metadata; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA  02110-1301  USA
 */

package no.met.metsis.metadata;

import com.beust.jcommander.IParameterValidator;
import com.beust.jcommander.ParameterException;
import java.nio.file.Files;
import java.nio.file.Paths;

public class FilePathValidator implements IParameterValidator {

    @Override
    public void validate(String name, String value) throws ParameterException {
        if (("--sourceDirectory".equals(name) || "--destinationDirectory".equals(name)) && !Files.exists(Paths.get(value))) {
            throw new ParameterException(name.substring(2) + "[" + value + "] does not exist!");
        }
        if ("--xslt".equals(name) && (!Paths.get(value).isAbsolute() || !Files.isRegularFile(Paths.get(value)))) {
            throw new ParameterException("Should provide a file with absolute path!");
        }
        if ("--metadataFile".equals(name) && (!Paths.get(value).isAbsolute() || !Files.isRegularFile(Paths.get(value)))) {
            throw new ParameterException("Should provide a file with absolute path!");
        }
    }

}
