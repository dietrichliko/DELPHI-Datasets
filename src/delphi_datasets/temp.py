def methodology(format: str, year: str, data: bool, version: str) -> dict[str, str]:
    """Description of the data."""
    
    if data:
        description = f"The data was recorded by the DELPHI detector in the year {year}."
    else:
        description = f"The data was simulated by DELSIM for the DELPHI detector configuration of the year {year}."
        
    if format == "DSTO":
        description += f"It was then reconstruced by the detector reconstuction program DELANA (Version {version})."
    elif format in ['SHORT', 'LONG', 'XSHORT']:
        description += (
                        "It was then reconstruced by the detector reconstuction program DELANA and the "
                        f"physics DST program PXDST(Version {version})."
        )

        
    return { "description": description }
        
def usage(fromat: str) -> dict[str, str | dict[str, str]]:
    """Usage of the data."""
    
    if format == "RAWD":
        description = "The RAW data is availabe for processing with the event server for visaltion with DELGRA",
        links = [
            {
                "description": "The DELPHI Event Server",
                "url": "/docs/delphi-guide-eventserver",
            },
            {
                "description": "The DELPHI Event Display Manual",
                "url": "/record/80503",
            },          
        ]
    elif format == "DSTO":
        description = "The detector data is availabe for visaltion with DELGRA",
        links = [
            {
                "description": "The DELPHI Event Display Manual",
                "url": "/record/80503",
            }
        ]    
    elif format in [ "SHORT", "LONG", "XSHORT"]:
        description = f"The DST data in the {format} format is availabe for anaysis.",
        links = [{
            "description": "Getting started with DELPHI data",
            "url": "/docs/delphi-getting-started",
        }, {
            "description": "DELPHI skeleton analysis framework manual":
            "url": "/record/80502",
        }]
        if format == "SHORT":
            links.append({
                "description": "DELPHI \"short DST\" manual",
                "url": "/record/80506"
            })
        elif format == "LONG":
            links.append({
                "description": "DELPHI \"full DST\" manuals",
                "url": "/record/80504"
            })
        elif format == "XSHORT":
            links.append({
                "description": "DELPHI \"extended short DST\" manual",
                "url": "/record/80505"
            }) 
            
    return { 
            "description": description,
            "links": links,
    }           
    