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

import com.fasterxml.jackson.annotation.JsonAnySetter;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import org.jdom2.Element;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
public final class IndexScehmaField {
    private static final Logger LOGGER = LoggerFactory.getLogger(IndexScehmaField.class.getName());
    
    private String name;
    private String type;
    private boolean indexed = true;
    private boolean stored = true;
    private boolean required;
    private boolean multiValued;

    public IndexScehmaField() {
    }

    public IndexScehmaField(String name, String type) {
        this.name = name;
        this.type = type;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public boolean isIndexed() {
        return indexed;
    }

    public void setIndexed(boolean indexed) {
        this.indexed = indexed;
    }

    public boolean isStored() {
        return stored;
    }

    public void setStored(boolean stored) {
        this.stored = stored;
    }

    public boolean isRequired() {
        return required;
    }

    public void setRequired(boolean required) {
        this.required = required;
    }

    public boolean isMultiValued() {
        return multiValued;
    }

    public void setMultiValued(boolean multiValued) {
        this.multiValued = multiValued;
    }      

    public Element toElement() {
        Element field = new Element("field");
        field.setAttribute("name", name);
        if (type != null)
            field.setAttribute("type", type);        
        field.setAttribute("indexed", String.valueOf(indexed));
        field.setAttribute("stored", String.valueOf(stored));
        field.setAttribute("required", String.valueOf(required));
        field.setAttribute("multiValued", String.valueOf(multiValued));
        return field;
    }
    
    public String toJson() throws JsonProcessingException {
        ObjectWriter ow = new ObjectMapper().writer().withDefaultPrettyPrinter();
        return ow.writeValueAsString(this);
    }
    
    @JsonAnySetter
    public void handleUnknown(String key, Object value) {
            LOGGER.trace("Unknown properties [{0} = {1}]", new Object[]{key, value});
    }  

    @Override
    public String toString() {
        return "<field " + "name=\"" + name + "\" type=\"" + type + "\" indexed=\"" + indexed + "\" stored=\"" + stored 
                + "\" required=\"" + required + "\" multiValued=\"" + multiValued + "\" />";
    } 
    
}
