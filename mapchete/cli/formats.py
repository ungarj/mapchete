"""CLI to format drivers."""

from mapchete.formats import available_input_formats, available_output_formats


def list_formats(args):
    """List input and/or output formats."""
    if args.input_formats == args.output_formats:
        show_inputs, show_outputs = True, True
    else:
        show_inputs, show_outputs = args.input_formats, args.output_formats

    if show_inputs:
        print "input formats:"
        for driver in available_input_formats():
            print "-", driver
    if show_outputs:
        print "output formats:"
        for driver in available_output_formats():
            print "-", driver
