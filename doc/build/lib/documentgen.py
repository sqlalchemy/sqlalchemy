import sys, re, os
import myghty.interp
import myghty.exception as exception

# document generation library

def genall(comps, component_root, output_dir):
    interp = myghty.interp.Interpreter( component_root = component_root)
    
    try:
        for comp in comps:
            gendoc(comp, interp, output_dir = output_dir)
    except exception.Error, e:
        sys.stderr.write(e.textformat())


def gendoc(doccomp, interp, output_dir):
    component = interp.load(doccomp)
    files = component.get_attribute('files')
    index = component.get_attribute('index')
    onepage = component.get_attribute('onepage')

    genfile(index + ".myt", interp, output_dir)

    for file in files:
        file += '.myt'
        genfile(file, interp, output_dir)

    genfile(index + ".myt", interp, output_dir, outfile = onepage + ".html", args = {'paged':'no'})



def genfile(file, interp, output_dir, outfile = None, args = {}):
    if outfile is None:
        outfile = re.sub(r"\..+$", "%s" % '.html', file)

    outfile = os.path.join(output_dir, outfile)
    print "%s -> %s" % (file, outfile)
    outbuf = open(outfile, "w")

    interp.execute(file, out_buffer = outbuf, request_args = args, raise_error = True)
        
    outbuf.close()

