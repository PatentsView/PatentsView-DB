from lxml import etree
from typing import Iterator

def document_text_generator(xml_file: str) -> Iterator[str]:
    """
    generator that sequentially yields each xml document within an xml file as a string
    :param xml_file: the path to the xml file to iteratively extract text from
    """
    xml_marker = '<?xml version="1.0"'
    current_document_lines = []
    with open(xml_file, "r") as freader:
        # Loop through all the lines in the file
        for line in freader:
            # Determine the start of a new document
            if xml_marker in line:
                current_document_lines.append(line.split(xml_marker)[0])
                # Join all lines for a given document
                current_xml = "".join(current_document_lines)
                if current_xml.strip() != '':
                    yield current_xml
                current_document_lines = []
                current_document_lines.append(xml_marker)
                current_document_lines.append(line.split(xml_marker)[-1])
            else:
                current_document_lines.append(line)
        current_xml = "".join(current_document_lines)
        yield current_xml

def extract_specific_doc_text(xml_file:str, doc_number:str) -> str :
    """
    returns an LXML etree containing the document specified by the document number within the provided xml file if it exists
    :param xml_file: the path to the xml to search for the desired document
    :param doc_number: the document number or patent id of the desired document
    """
    for doc in document_text_generator(xml_file):
        if f'{doc_number}' in doc:
            return(doc)
    print(f"""
    document number {doc_number} not found in {xml_file}.
    check that filename and document number are correct.
    note that Patents with a letter prefix may have a leading zero removed from the numeric part of the ID.""")
    return None

def extract_specific_doc_tree(xml_file:str, doc_number:str) -> etree.Element :
    """
    returns an LXML etree containing the document specified by the document number within the provided xml file if it exists
    :param xml_file: the path to the xml to search for the desired document
    :param doc_number: the document number or patent id of the desired document
    """
    for doc in document_text_generator(xml_file):
        if f'{doc_number}' in doc:
            tree = etree.XML(doc.encode('utf-8'), parser=etree.XMLParser(no_network=False))
            return(tree)
    print(f"""
    document number {doc_number} not found in {xml_file}.
    check that filename and document number are correct.
    note that Patents with a letter prefix may have a leading zero removed from the numeric part of the ID.""")
    return None

def tree_generator(xml_file:str) -> Iterator[etree.Element] :
    """
    generator that yields each xml document within an xml file as an lxml etree object, skipping over sequence-cwu documents
    :param xml_file: the path to the xml file to iteratively produce etrees from
    """
    for doc in document_text_generator(xml_file):
        tree = etree.XML(doc.encode('utf-8'), parser=etree.XMLParser(no_network=False))
        if tree.tag == 'sequence-cwu':
            continue
        yield tree
